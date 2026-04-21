"""
zbhelper.ingest_v2 — setup_zerobus / call_zerobus_insert for all 6 modes.

Mirrors cells 20–32 of notebooks/zerobus_grpc_http.ipynb with workspace globals
replaced by an explicit ``cfg`` dict so the same logic works in both notebooks
(inline copy) and the benchmark driver (imported via zbhelper).

cfg keys (all strings):
    server_endpoint      – gRPC server address
    workspace_url        – https://…
    workspace_id         – numeric string
    zerobus_ingest_url   – https://…
    client_id
    client_secret
    catalog
    schema
    table_name           – fully-qualified catalog.schema.table
"""

from __future__ import annotations

import asyncio
import datetime
import json
import time
from pathlib import Path
from statistics import mean, median
from typing import Any, Optional

# ---------------------------------------------------------------------------
# Mode constants (mirrors zerobus_grpc_http.ipynb cell 7)
# ---------------------------------------------------------------------------

MODE_MAP: dict[tuple[str, str], str] = {
    ("grpc",    "sync"):  "grpc_sync",
    ("grpc",    "async"): "grpc_async",
    ("http/1.1","sync"):  "http_sync",
    ("http/1.1","async"): "http_async",
    ("http/2",  "sync"):  "http2_sync",
    ("http/2",  "async"): "http2_async",
}

MODE_LABEL: dict[str, str] = {
    "grpc_sync":   "gRPC sync",
    "grpc_async":  "gRPC async",
    "http_sync":   "HTTP/1.1 sync",
    "http_async":  "HTTP/1.1 async",
    "http2_sync":  "HTTP/2 sync",
    "http2_async": "HTTP/2 async",
}

# ---------------------------------------------------------------------------
# Record construction (same as ingest_benchmark.build_records)
# ---------------------------------------------------------------------------

def build_records(n: int, offset: int = 0) -> list[dict]:
    return [
        {"device_name": f"sensor-{i}", "temp": 20 + i % 15, "humidity": 50 + i % 40}
        for i in range(offset, offset + n)
    ]


# ---------------------------------------------------------------------------
# Spark helpers (same logic as ingest_benchmark)
# ---------------------------------------------------------------------------

def fetch_row_baseline(spark: Any, table_name: str) -> dict:
    count = spark.sql(f"SELECT COUNT(*) AS c FROM {table_name}").collect()[0]["c"]
    bounds = spark.sql(
        f"SELECT MIN(device_name) AS lo, MAX(device_name) AS hi FROM {table_name}"
    ).collect()[0]
    return {"count": count, "lo": bounds["lo"], "hi": bounds["hi"]}


def poll_visibility(
    spark: Any,
    table_name: str,
    target_count: int,
    t_after_close: float,
    t_4a0: float,
    timeout: float = 120.0,
) -> dict:
    """Poll COUNT(*) until target_count or timeout. Returns dict with timing."""
    deadline = t_after_close + timeout
    row_visible = 0
    while time.perf_counter() < deadline:
        row_visible = spark.sql(f"SELECT COUNT(*) AS c FROM {table_name}").collect()[0]["c"]
        if row_visible >= target_count:
            break
        time.sleep(0.15)
    t_visible = time.perf_counter()
    bounds = spark.sql(
        f"SELECT MIN(device_name) AS lo, MAX(device_name) AS hi FROM {table_name}"
    ).collect()[0]
    return {
        "target_count": target_count,
        "row_visible": row_visible,
        "visibility_s": t_visible - t_after_close,
        "visibility_from_first_send_s": t_visible - t_4a0,
        "bounds_lo": bounds["lo"],
        "bounds_hi": bounds["hi"],
    }


# ---------------------------------------------------------------------------
# OAuth token cache (keyed by client_id + table_name)
# ---------------------------------------------------------------------------

_token_cache: dict[tuple, dict] = {}  # key → {"token": str, "expires_at": float}


def _get_cached_token(
    workspace_url: str,
    workspace_id: str,
    client_id: str,
    client_secret: str,
    catalog: str,
    schema: str,
    table_name: str,
    margin: float = 60.0,
) -> tuple[str, float]:
    """Return (access_token, oauth_seconds).

    Fetches a new token only when the cached one is absent or within
    `margin` seconds of expiry. `oauth_seconds` is 0.0 on a cache hit.
    """
    import requests as _req
    from requests.auth import HTTPBasicAuth as _BA

    key = (client_id, table_name)
    entry = _token_cache.get(key)
    if entry and time.monotonic() < entry["expires_at"] - margin:
        return entry["token"], 0.0

    auth_details = json.dumps([
        {"type": "unity_catalog_privileges", "privileges": ["USE CATALOG"],
         "object_type": "CATALOG",  "object_full_path": catalog},
        {"type": "unity_catalog_privileges", "privileges": ["USE SCHEMA"],
         "object_type": "SCHEMA",   "object_full_path": f"{catalog}.{schema}"},
        {"type": "unity_catalog_privileges", "privileges": ["SELECT", "MODIFY"],
         "object_type": "TABLE",    "object_full_path": table_name},
    ])
    t0 = time.perf_counter()
    resp = _req.post(
        f"{workspace_url}/oidc/v1/token",
        auth=_BA(client_id, client_secret),
        data={
            "grant_type": "client_credentials",
            "scope": "all-apis",
            "resource": f"api://databricks/workspaces/{workspace_id}/zerobusDirectWriteApi",
            "authorization_details": auth_details,
        },
        timeout=60,
    )
    resp.raise_for_status()
    body = resp.json()
    elapsed = time.perf_counter() - t0
    _token_cache[key] = {
        "token":      body["access_token"],
        "expires_at": time.monotonic() + body.get("expires_in", 3600),
    }
    print(f"Fetched OAuth token in {elapsed * 1000:.1f} ms")
    return body["access_token"], elapsed


# ---------------------------------------------------------------------------
# Ping helper (mirrors zerobus_grpc_http.ipynb cell 21)
# ---------------------------------------------------------------------------

def ping_endpoint(url: str, timeout: float = 10.0) -> float:
    """TCP connect to host:443 and return elapsed seconds (1 network RTT, no TLS/HTTP)."""
    import socket
    from urllib.parse import urlparse
    parsed = urlparse(url)
    host = parsed.hostname
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    t0 = time.perf_counter()
    try:
        with socket.create_connection((host, port), timeout=timeout):
            pass
    except Exception:
        pass
    return time.perf_counter() - t0


# ---------------------------------------------------------------------------
# ZeroBus warm GET ping (HTTP modes only)
# ---------------------------------------------------------------------------

async def http_ping(client: dict, url: str, timeout: float = 10.0) -> Optional[float]:
    """GET url on the already-open session and return elapsed seconds.

    Returns None for gRPC modes (no HTTP session).
    Any HTTP response (incl. 4xx/5xx) counts — only timing matters.
    """
    mode = client.get("mode", "")
    session = client.get("session")
    if session is None:
        return None
    t0 = time.perf_counter()
    try:
        if mode in ("http_sync", "http2_sync"):
            session.get(url, timeout=timeout)
        elif mode == "http2_async":
            # httpx.AsyncClient.get() is a plain coroutine, not a context manager
            await session.get(url, timeout=timeout)
        else:
            # aiohttp.ClientSession.get() is an async context manager
            import aiohttp as _aiohttp
            async with session.get(url, timeout=_aiohttp.ClientTimeout(total=timeout)) as _r:
                pass
    except Exception:
        pass
    return time.perf_counter() - t0


# ---------------------------------------------------------------------------
# AckCallback helper
# ---------------------------------------------------------------------------

def _make_ack_callback(ack_events: list):
    try:
        from zerobus.sdk.shared import AckCallback
    except ImportError:
        return None, ack_events

    class _CB(AckCallback):
        def on_ack(self, offset: int) -> None:
            print(f"[ack callback] offset {offset} acknowledged")
            ack_events.append(("ack", offset))

        def on_error(self, offset: int, error_message: str) -> None:
            print(f"[ack callback] error at offset {offset}: {error_message}")
            ack_events.append(("error", offset, error_message))

    return _CB(), ack_events


# ---------------------------------------------------------------------------
# setup_zerobus (mirrors zerobus_grpc_http.ipynb cell 20)
# ---------------------------------------------------------------------------

async def setup_zerobus(mode: str, cfg: dict) -> dict:
    """Open a Zerobus stream or HTTP session for the given mode.

    cfg keys: server_endpoint, workspace_url, workspace_id, zerobus_ingest_url,
              client_id, client_secret, catalog, schema, table_name.

    Returns a client dict with keys:
      mode, stream, session, close, _connect_s, _oauth_s,
      _ack_events (gRPC only), _access_token/_rest_insert_url (HTTP only).
    """
    server_endpoint       = cfg["server_endpoint"]
    workspace_url         = cfg["workspace_url"]
    workspace_id          = cfg["workspace_id"]
    zerobus_ingest_url    = cfg["zerobus_ingest_url"]
    client_id             = cfg["client_id"]
    client_secret         = cfg["client_secret"]
    catalog               = cfg["catalog"]
    schema                = cfg["schema"]
    table_name            = cfg["table_name"]

    if mode == "grpc_sync":
        from zerobus.sdk.sync import ZerobusSdk
        from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties

        ack_events: list = []
        cb, ack_events = _make_ack_callback(ack_events)

        sdk = ZerobusSdk(server_endpoint, workspace_url)
        table_properties = TableProperties(table_name)
        options = StreamConfigurationOptions(record_type=RecordType.JSON, ack_callback=cb)
        t0 = time.perf_counter()
        stream = sdk.create_stream(client_id, client_secret, table_properties, options)
        connect_s = time.perf_counter() - t0

        async def _close():
            stream.close()

        return {
            "mode": mode, "stream": stream, "session": None, "close": _close,
            "_connect_s": connect_s, "_oauth_s": None, "_ack_events": ack_events,
        }

    elif mode == "grpc_async":
        try:
            from zerobus.sdk.aio import ZerobusSdkAsync
        except ImportError:
            from zerobus.sdk.aio import ZerobusSdk as ZerobusSdkAsync  # type: ignore[assignment]
        from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties

        ack_events = []
        cb, ack_events = _make_ack_callback(ack_events)

        sdk = ZerobusSdkAsync(server_endpoint, workspace_url)
        table_properties = TableProperties(table_name)
        options = StreamConfigurationOptions(record_type=RecordType.JSON, ack_callback=cb)
        t0 = time.perf_counter()
        stream = await sdk.create_stream(client_id, client_secret, table_properties, options)
        connect_s = time.perf_counter() - t0

        async def _close():  # type: ignore[no-redef]
            await stream.close()

        return {
            "mode": mode, "stream": stream, "session": None, "close": _close,
            "_connect_s": connect_s, "_oauth_s": None, "_ack_events": ack_events,
        }

    elif mode in ("http_sync", "http_async", "http2_sync", "http2_async"):
        import requests

        access_token, oauth_s = _get_cached_token(
            workspace_url, workspace_id, client_id, client_secret,
            catalog, schema, table_name,
        )
        if oauth_s == 0.0:
            print("OAuth token reused from cache")

        rest_url = f"{zerobus_ingest_url}/zerobus/v1/tables/{table_name}/insert"

        if mode == "http_sync":
            session = requests.Session()
            t0 = time.perf_counter()
            try:
                session.get(zerobus_ingest_url, timeout=30)
            except Exception:
                pass
            connect_s = time.perf_counter() - t0
            print(f"[http_sync] TCP+TLS connected in {connect_s * 1000:.1f} ms")

            async def _close():  # type: ignore[no-redef]
                session.close()

        elif mode == "http_async":
            import aiohttp
            session = aiohttp.ClientSession()
            t0 = time.perf_counter()
            try:
                async with session.get(zerobus_ingest_url, timeout=aiohttp.ClientTimeout(total=30)) as _r:
                    pass
            except Exception:
                pass
            connect_s = time.perf_counter() - t0
            print(f"[http_async] TCP+TLS connected in {connect_s * 1000:.1f} ms")

            async def _close():  # type: ignore[no-redef]
                await session.close()

        elif mode == "http2_sync":
            import httpx
            session = httpx.Client(http2=True)
            t0 = time.perf_counter()
            try:
                session.get(zerobus_ingest_url, timeout=30)
            except Exception:
                pass
            connect_s = time.perf_counter() - t0
            print(f"[http2_sync] TCP+TLS+h2 connected in {connect_s * 1000:.1f} ms")

            async def _close():  # type: ignore[no-redef]
                session.close()

        else:  # http2_async
            import httpx
            session = httpx.AsyncClient(http2=True)
            t0 = time.perf_counter()
            try:
                await session.get(zerobus_ingest_url, timeout=30)
            except Exception:
                pass
            connect_s = time.perf_counter() - t0
            print(f"[http2_async] TCP+TLS+h2 connected in {connect_s * 1000:.1f} ms")

            async def _close():  # type: ignore[no-redef]
                await session.aclose()

        _cache_key = (client_id, table_name)

        def _refresh_access_token() -> str:
            _token_cache.pop(_cache_key, None)
            token, _ = _get_cached_token(
                workspace_url, workspace_id, client_id, client_secret,
                catalog, schema, table_name,
            )
            return token

        return {
            "mode": mode, "stream": None, "session": session, "close": _close,
            "_connect_s": connect_s, "_oauth_s": oauth_s,
            "_access_token": access_token, "_rest_insert_url": rest_url,
            "_refresh_access_token": _refresh_access_token,
        }

    else:
        raise ValueError(f"Unknown mode: {mode!r}")


# ---------------------------------------------------------------------------
# call_zerobus_insert (mirrors zerobus_grpc_http.ipynb cell 20)
# ---------------------------------------------------------------------------

async def call_zerobus_insert(client: dict, rows: list) -> tuple[float, float]:
    """Insert rows using the client returned by setup_zerobus.

    Returns (send_seconds, wait_seconds). HTTP wait is always 0.0.
    """
    mode = client["mode"]

    if mode == "grpc_sync":
        stream = client["stream"]
        t_send = time.perf_counter()
        offset = stream.ingest_record_offset(rows[0]) if len(rows) == 1 else stream.ingest_records_offset(rows)
        send_s = time.perf_counter() - t_send
        t_wait = time.perf_counter()
        stream.wait_for_offset(offset)
        return send_s, time.perf_counter() - t_wait

    elif mode == "grpc_async":
        stream = client["stream"]
        t_send = time.perf_counter()
        if len(rows) == 1:
            offset = await stream.ingest_record_offset(rows[0])
        else:
            offset = await stream.ingest_records_offset(rows)
        send_s = time.perf_counter() - t_send
        t_wait = time.perf_counter()
        await stream.wait_for_offset(offset)
        return send_s, time.perf_counter() - t_wait

    elif mode == "http_sync":
        body = json.dumps(rows)
        for _attempt in range(2):
            headers = {"Content-Type": "application/json", "Authorization": f"Bearer {client['_access_token']}"}
            t0 = time.perf_counter()
            resp = client["session"].post(client["_rest_insert_url"], headers=headers, data=body, timeout=120)
            if resp.status_code == 401 and _attempt == 0:
                client["_access_token"] = client["_refresh_access_token"]()
                continue
            resp.raise_for_status()
            return time.perf_counter() - t0, 0.0

    elif mode == "http_async":
        import aiohttp
        body = json.dumps(rows)
        for _attempt in range(2):
            headers = {"Content-Type": "application/json", "Authorization": f"Bearer {client['_access_token']}"}
            t0 = time.perf_counter()
            async with client["session"].post(
                client["_rest_insert_url"], data=body, headers=headers,
                timeout=aiohttp.ClientTimeout(total=120),
            ) as resp:
                if resp.status == 401 and _attempt == 0:
                    client["_access_token"] = client["_refresh_access_token"]()
                    continue
                resp.raise_for_status()
            return time.perf_counter() - t0, 0.0

    elif mode == "http2_sync":
        body = json.dumps(rows)
        for _attempt in range(2):
            headers = {"Content-Type": "application/json", "Authorization": f"Bearer {client['_access_token']}"}
            t0 = time.perf_counter()
            resp = client["session"].post(client["_rest_insert_url"], content=body, headers=headers, timeout=120)
            if resp.status_code == 401 and _attempt == 0:
                client["_access_token"] = client["_refresh_access_token"]()
                continue
            resp.raise_for_status()
            return time.perf_counter() - t0, 0.0

    elif mode == "http2_async":
        body = json.dumps(rows)
        for _attempt in range(2):
            headers = {"Content-Type": "application/json", "Authorization": f"Bearer {client['_access_token']}"}
            t0 = time.perf_counter()
            resp = await client["session"].post(client["_rest_insert_url"], content=body, headers=headers, timeout=120)
            if resp.status_code == 401 and _attempt == 0:
                client["_access_token"] = client["_refresh_access_token"]()
                continue
            resp.raise_for_status()
            return time.perf_counter() - t0, 0.0

    else:
        raise ValueError(f"Unknown mode in client: {mode!r}")


# ---------------------------------------------------------------------------
# Metrics helpers (mirrors zerobus_grpc_http.ipynb cells 31–32)
# ---------------------------------------------------------------------------

def ms(seconds: float) -> float:
    """Convert seconds to rounded milliseconds."""
    return round(seconds * 1000, 3)


def stats_ms(values: list[float]) -> Optional[dict]:
    """Return {min, mean, median, max} in ms, or None if list is empty."""
    if not values:
        return None
    return {
        "min":    ms(min(values)),
        "mean":   ms(mean(values)),
        "median": ms(median(values)),
        "max":    ms(max(values)),
    }


def append_jsonl(result: dict, path: Path | str) -> None:
    """Append one JSON record to a .jsonl file (creates file + parents if needed)."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "a") as fh:
        fh.write(json.dumps(result) + "\n")


# ---------------------------------------------------------------------------
# Pandas flatten (mirrors zerobus_grpc_http.ipynb cell 32)
# ---------------------------------------------------------------------------

_COMMON_COLS = [
    "run_at", "mode", "mode_label", "table", "zerobus_endpoint", "n", "concurrency",
    "oauth_ms", "ping_ms", "http_ping_ms", "connect_ms", "disconnect_ms",
    "ingest_wall_ms", "visibility_from_first_send_ms", "visibility_from_end_ms",
]


def _expand_stats(rec: dict, key: str, src: Any) -> None:
    if isinstance(src, dict):
        for stat in ("min", "mean", "median", "max"):
            rec[f"{key}_{stat}"] = src.get(stat)


def flatten_jsonl_to_df(path: Path | str):
    """Read a benchmark_results.jsonl and return a flat pandas DataFrame."""
    import pandas as pd

    raw = pd.read_json(path, lines=True)
    rows = []
    for _, r in raw.iterrows():
        base = {c: r.get(c) for c in _COMMON_COLS}
        for label in ("4a", "4b"):
            d = r.get(label)
            if not isinstance(d, dict):
                continue
            rec = {**base, "scenario": label}
            rec["rows"] = d.get("rows")
            rec["runs"] = d.get("runs")
            wall = d.get("wall_ms")
            if wall is not None:
                rec["wall_ms"] = wall
            _expand_stats(rec, "send_wait_ms", d.get("send_wait_ms"))
            _expand_stats(rec, "send_ms",      d.get("send_ms"))
            _expand_stats(rec, "wait_ms",      d.get("wait_ms"))
            rows.append(rec)
    return pd.DataFrame(rows)


def display_results(path: Path | str) -> None:
    """Flatten the JSONL, aggregate multiple runs by median, and display."""
    import pandas as pd

    pd.set_option("display.max_columns", None)
    pd.set_option("display.float_format", "{:,.0f}".format)

    df = flatten_jsonl_to_df(path)
    if df.empty:
        print("No results yet.")
        return

    df["concurrency"] = df["concurrency"].fillna(1).astype(int)
    _GROUP = ["mode", "concurrency", "scenario"]
    run_counts = df.groupby(_GROUP).size().reset_index(name="run_count")
    if run_counts["run_count"].max() > 1:
        num  = [c for c in df.select_dtypes(include="number").columns if c not in _GROUP]
        strs = [c for c in df.columns if c not in num and c not in _GROUP and c != "run_at"]
        df = (
            df.groupby(_GROUP, sort=False)
            .agg({**{c: "median" for c in num}, **{c: "first" for c in strs}})
            .reset_index()
            .merge(run_counts, on=_GROUP)
        )
        df = df.drop(columns=["run_at"], errors="ignore")
        front = ["scenario", "rows", "runs", "run_count", "mode_label", "concurrency"]
    else:
        front = ["scenario", "rows", "runs", "mode_label", "concurrency", "run_at"]

    rest = [c for c in df.columns if c not in front]
    df = df[front + rest]
    try:
        from IPython.display import display as _display
        _display(df)
    except ImportError:
        print(df.to_string())


def write_csv_from_df(df, path: Path | str) -> None:
    """Write the flat DataFrame to CSV."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    print(f"Wrote CSV → {path}  ({len(df)} rows)")
