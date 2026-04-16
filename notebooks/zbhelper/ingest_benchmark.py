"""
Zerobus ingest benchmark — JSON record patterns from the four demo notebooks.

Each function mirrors one Step-4 cell from the demo notebooks so that notebooks
can be retrofitted to call these functions directly.  All Databricks runtime
context (spark, dbutils, WorkspaceClient) is passed in as arguments; nothing
from the Databricks runtime is imported at module level.

Notebook mapping
----------------
Cell 18 (setup)  → fetch_row_baseline, resolve_sp_config,
                   open_grpc_stream_sync / open_grpc_stream_async,
                   fetch_http_token, http_insert_url
Cell 20 (4a)     → ingest_singles_grpc_sync / _grpc_async / _http_sync / _http_async
Cell 22 (4b)     → ingest_batch_and_close_grpc_sync / _grpc_async /
                   ingest_batch_http_sync / _http_async
Cell 24 (4c)     → poll_visibility
Cell 26 (4d)     → print_metrics
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from statistics import mean, median
from typing import Any, Optional

# ---------------------------------------------------------------------------
# Optional SDK imports — not available outside a Databricks / SDK environment.
# Module loads cleanly without them; errors surface at call time.
# ---------------------------------------------------------------------------
try:
    from zerobus.sdk.sync import ZerobusSdk
except ImportError:  # pragma: no cover
    ZerobusSdk = None  # type: ignore[assignment,misc]

try:
    from zerobus.sdk.aio import ZerobusSdkAsync
except ImportError:  # pragma: no cover
    try:
        from zerobus.sdk.aio import ZerobusSdk as ZerobusSdkAsync  # type: ignore[assignment]
    except ImportError:
        ZerobusSdkAsync = None  # type: ignore[assignment,misc]

try:
    from zerobus.sdk.shared import AckCallback, RecordType, StreamConfigurationOptions, TableProperties
except ImportError:  # pragma: no cover
    AckCallback = object  # type: ignore[assignment,misc]
    RecordType = None  # type: ignore[assignment,misc]
    StreamConfigurationOptions = None  # type: ignore[assignment,misc]
    TableProperties = None  # type: ignore[assignment,misc]

try:
    import aiohttp as _aiohttp
except ImportError:  # pragma: no cover
    _aiohttp = None  # type: ignore[assignment]

try:
    import requests as _requests
    from requests.auth import HTTPBasicAuth as _HTTPBasicAuth
except ImportError:  # pragma: no cover
    _requests = None  # type: ignore[assignment]
    _HTTPBasicAuth = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------

@dataclass
class SinglesResult:
    """Timing output from the 4a single-row ingest loop (cell 20)."""

    singles_n: int
    singles_wall_s: float
    row_send_seconds: list[float]
    row_wait_seconds: list[float]
    row_ack_seconds: list[float]
    bytes_4a: int
    t_4a0: float  # perf_counter at first row send — used by poll_visibility


@dataclass
class BatchResult:
    """Timing output from the 4b batch ingest + close step (cell 22)."""

    batch_n: int
    batch_send_s: Optional[float]   # None when batch_n == 0
    batch_wait_s: Optional[float]   # None when batch_n == 0; 0.0 for HTTP
    batch_ack_s: Optional[float]    # None when batch_n == 0
    stream_close_s: Optional[float] # None for HTTP (no stream)
    t_after_close: float            # perf_counter at end of ingest — used by poll_visibility
    bytes_4b: int
    stream_open_s: Optional[float]  # propagated from setup for print_metrics convenience


@dataclass
class VisibilityResult:
    """Output from the 4c visibility poll (cell 24)."""

    target_count: int
    row_visible: int
    visibility_s: float                  # end of ingest → COUNT(*) reached target
    visibility_from_first_send_s: float  # first row send → COUNT(*) reached target
    bounds_lo: Optional[str]             # MIN(device_name) after visibility confirmed
    bounds_hi: Optional[str]             # MAX(device_name) after visibility confirmed


# ---------------------------------------------------------------------------
# Shared utilities
# ---------------------------------------------------------------------------

def json_payload_bytes(obj: dict) -> int:
    """UTF-8 byte length of compact JSON — mirrors _json_payload_bytes in notebooks."""
    return len(json.dumps(obj, separators=(",", ":")).encode("utf-8"))


def build_records(n: int, offset: int = 0) -> list[dict]:
    """
    Build n demo device records starting at index offset.

    Matches the record_dict shape used in all four notebooks:
      {"device_name": "sensor-{i}", "temp": 20 + i % 15, "humidity": 50 + i % 40}
    """
    return [
        {
            "device_name": f"sensor-{i}",
            "temp": 20 + i % 15,
            "humidity": 50 + i % 40,
        }
        for i in range(offset, offset + n)
    ]


# ---------------------------------------------------------------------------
# Secrets and baseline
# ---------------------------------------------------------------------------

def resolve_sp_config(
    dbutils: Any,
    secret_scope: str,
    secret_key: str,
    oauth_field: str = "ZEROBUS_OAUTH_SECRET",
) -> tuple[str, str]:
    """
    Read CLIENT_ID and CLIENT_SECRET from a Databricks secret JSON.

    Mirrors the dbutils.secrets.get + json.loads pattern in Step 2.b/2.c.
    Returns (client_id, client_secret) where client_id is ZEROBUS_APP_ID.
    """
    raw = dbutils.secrets.get(scope=secret_scope, key=secret_key)
    cfg = json.loads(raw)
    return cfg["ZEROBUS_APP_ID"], cfg[oauth_field]


def fetch_row_baseline(spark: Any, table_name: str) -> dict:
    """
    Run COUNT(*) + MIN/MAX against table_name.

    Returns {"count": int, "lo": str | None, "hi": str | None}.
    Mirrors the two spark.sql calls at the top of cell 18 in every notebook.
    """
    count = spark.sql(f"SELECT COUNT(*) AS c FROM {table_name}").collect()[0]["c"]
    bounds = spark.sql(
        f"SELECT MIN(device_name) AS lo, MAX(device_name) AS hi FROM {table_name}"
    ).collect()[0]
    return {"count": count, "lo": bounds["lo"], "hi": bounds["hi"]}


# ---------------------------------------------------------------------------
# gRPC shared: AckCallback
# ---------------------------------------------------------------------------

class DemoBenchmarkAckCallback(AckCallback):
    """
    Log each server ack and append to an event list.

    Mirrors DemoAckCallback in the gRPC notebooks.  Pass ack_events=[] from
    the driver so events are accessible after the stream closes.
    """

    def __init__(self, ack_events: list) -> None:
        super().__init__()
        self._events = ack_events

    def on_ack(self, offset: int) -> None:
        print(f"[ack callback] offset {offset} acknowledged")
        self._events.append(("ack", offset))

    def on_error(self, offset: int, error_message: str) -> None:
        print(f"[ack callback] error at offset {offset}: {error_message}")
        self._events.append(("error", offset, error_message))


# ---------------------------------------------------------------------------
# gRPC sync — open stream
# ---------------------------------------------------------------------------

def open_grpc_stream_sync(
    server_endpoint: str,
    workspace_url: str,
    client_id: str,
    client_secret: str,
    table_name: str,
    ack_events: Optional[list] = None,
) -> tuple[Any, float]:
    """
    Create a ZerobusSdk (sync) stream and return (stream, stream_open_s).

    Mirrors the sdk / table_properties / options / create_stream block in
    grpc_sync cell 18.  ack_events defaults to a fresh list if not supplied.
    """
    if ZerobusSdk is None:
        raise ImportError("zerobus.sdk.sync not installed")

    events: list = [] if ack_events is None else ack_events
    sdk = ZerobusSdk(server_endpoint, workspace_url)
    table_properties = TableProperties(table_name)
    options = StreamConfigurationOptions(
        record_type=RecordType.JSON,
        ack_callback=DemoBenchmarkAckCallback(events),
    )
    t0 = time.perf_counter()
    stream = sdk.create_stream(client_id, client_secret, table_properties, options)
    return stream, time.perf_counter() - t0


# ---------------------------------------------------------------------------
# gRPC sync — 4a singles (cell 20)
# ---------------------------------------------------------------------------

def ingest_singles_grpc_sync(stream: Any, records: list[dict]) -> SinglesResult:
    """
    Send each record individually via ingest_record_offset + wait_for_offset.

    Matches grpc_sync cell 20 exactly.
    """
    row_send_seconds: list[float] = []
    row_wait_seconds: list[float] = []
    row_ack_seconds: list[float] = []
    bytes_4a = 0

    t_4a0 = time.perf_counter()
    for record_dict in records:
        bytes_4a += json_payload_bytes(record_dict)
        t_row = time.perf_counter()
        offset = stream.ingest_record_offset(record_dict)
        t_row_wait = time.perf_counter()
        stream.wait_for_offset(offset)
        t_row_end = time.perf_counter()
        row_send_seconds.append(t_row_wait - t_row)
        row_wait_seconds.append(t_row_end - t_row_wait)
        row_ack_seconds.append(t_row_end - t_row)
    singles_wall_s = time.perf_counter() - t_4a0

    return SinglesResult(
        singles_n=len(records),
        singles_wall_s=singles_wall_s,
        row_send_seconds=row_send_seconds,
        row_wait_seconds=row_wait_seconds,
        row_ack_seconds=row_ack_seconds,
        bytes_4a=bytes_4a,
        t_4a0=t_4a0,
    )


# ---------------------------------------------------------------------------
# gRPC sync — 4b batch + close (cell 22)
# ---------------------------------------------------------------------------

def ingest_batch_and_close_grpc_sync(
    stream: Any,
    records: list[dict],
    stream_open_s: Optional[float] = None,
) -> BatchResult:
    """
    Send records as a single batch via ingest_records_offset + wait_for_offset,
    then close the stream.  records may be empty (stream still closed).

    Matches grpc_sync cell 22 exactly (try/finally pattern preserved).
    """
    batch_send_s: Optional[float] = None
    batch_wait_s: Optional[float] = None
    batch_ack_s: Optional[float] = None
    bytes_4b = 0

    try:
        if records:
            bytes_4b = sum(json_payload_bytes(r) for r in records)
            t_bs0 = time.perf_counter()
            offset = stream.ingest_records_offset(records)
            batch_send_s = time.perf_counter() - t_bs0
            t_bw0 = time.perf_counter()
            stream.wait_for_offset(offset)
            batch_wait_s = time.perf_counter() - t_bw0
            batch_ack_s = batch_send_s + batch_wait_s
    finally:
        t_close_start = time.perf_counter()
        stream.close()
        t_after_close = time.perf_counter()
        stream_close_s = t_after_close - t_close_start

    return BatchResult(
        batch_n=len(records),
        batch_send_s=batch_send_s,
        batch_wait_s=batch_wait_s,
        batch_ack_s=batch_ack_s,
        stream_close_s=stream_close_s,
        t_after_close=t_after_close,
        bytes_4b=bytes_4b,
        stream_open_s=stream_open_s,
    )


# ---------------------------------------------------------------------------
# gRPC async — open stream
# ---------------------------------------------------------------------------

async def open_grpc_stream_async(
    server_endpoint: str,
    workspace_url: str,
    client_id: str,
    client_secret: str,
    table_name: str,
    ack_events: Optional[list] = None,
) -> tuple[Any, float]:
    """
    Create a ZerobusSdkAsync stream and return (stream, stream_open_s).

    Matches grpc_async cell 18 (await sdk.create_stream).
    """
    if ZerobusSdkAsync is None:
        raise ImportError("zerobus.sdk.aio not installed")

    events: list = [] if ack_events is None else ack_events
    sdk = ZerobusSdkAsync(server_endpoint, workspace_url)
    table_properties = TableProperties(table_name)
    options = StreamConfigurationOptions(
        record_type=RecordType.JSON,
        ack_callback=DemoBenchmarkAckCallback(events),
    )
    t0 = time.perf_counter()
    stream = await sdk.create_stream(client_id, client_secret, table_properties, options)
    return stream, time.perf_counter() - t0


# ---------------------------------------------------------------------------
# gRPC async — 4a singles (cell 20)
# ---------------------------------------------------------------------------

async def ingest_singles_grpc_async(stream: Any, records: list[dict]) -> SinglesResult:
    """
    Pipeline all sends via await ingest_record_offset, then one barrier
    wait_for_offset after all rows.

    Matches grpc_async cell 20 exactly.
    """
    offsets_4a: list = []
    row_send_seconds: list[float] = []
    bytes_4a = 0

    t_4a0 = time.perf_counter()
    for record_dict in records:
        bytes_4a += json_payload_bytes(record_dict)
        t_row = time.perf_counter()
        offset = await stream.ingest_record_offset(record_dict)
        row_send_seconds.append(time.perf_counter() - t_row)
        offsets_4a.append(offset)
    singles_send_s = time.perf_counter() - t_4a0

    # async: barrier wait is not per-row
    row_wait_seconds = [0.0] * len(records)
    row_ack_seconds = list(row_send_seconds)

    singles_wait_ack_s = 0.0
    if offsets_4a:
        t_ack0 = time.perf_counter()
        await stream.wait_for_offset(offsets_4a[-1])
        singles_wait_ack_s = time.perf_counter() - t_ack0

    return SinglesResult(
        singles_n=len(records),
        singles_wall_s=singles_send_s + singles_wait_ack_s,
        row_send_seconds=row_send_seconds,
        row_wait_seconds=row_wait_seconds,
        row_ack_seconds=row_ack_seconds,
        bytes_4a=bytes_4a,
        t_4a0=t_4a0,
    )


# ---------------------------------------------------------------------------
# gRPC async — 4b batch + close (cell 22)
# ---------------------------------------------------------------------------

async def ingest_batch_and_close_grpc_async(
    stream: Any,
    records: list[dict],
    stream_open_s: Optional[float] = None,
) -> BatchResult:
    """
    Send records as a single batch then await stream.close().

    Matches grpc_async cell 22 exactly (try/finally pattern preserved).
    """
    batch_send_s: Optional[float] = None
    batch_wait_s: Optional[float] = None
    batch_ack_s: Optional[float] = None
    bytes_4b = 0

    try:
        if records:
            bytes_4b = sum(json_payload_bytes(r) for r in records)
            t_bs0 = time.perf_counter()
            offset = await stream.ingest_records_offset(records)
            batch_send_s = time.perf_counter() - t_bs0
            t_bw0 = time.perf_counter()
            await stream.wait_for_offset(offset)
            batch_wait_s = time.perf_counter() - t_bw0
            batch_ack_s = batch_send_s + batch_wait_s
    finally:
        t_close_start = time.perf_counter()
        await stream.close()
        t_after_close = time.perf_counter()
        stream_close_s = t_after_close - t_close_start

    return BatchResult(
        batch_n=len(records),
        batch_send_s=batch_send_s,
        batch_wait_s=batch_wait_s,
        batch_ack_s=batch_ack_s,
        stream_close_s=stream_close_s,
        t_after_close=t_after_close,
        bytes_4b=bytes_4b,
        stream_open_s=stream_open_s,
    )


# ---------------------------------------------------------------------------
# HTTP shared — token fetch and insert URL
# ---------------------------------------------------------------------------

def fetch_http_token(
    workspace_url: str,
    workspace_id: str,
    client_id: str,
    client_secret: str,
    catalog: str,
    schema: str,
    table_name: str,
) -> str:
    """
    Fetch a zerobusDirectWriteApi OAuth token via client_credentials.

    Matches the requests.post(…/oidc/v1/token…) block in http_sync/http_async
    cell 18 including the authorization_details UC privilege payload.
    """
    if _requests is None:
        raise ImportError("requests not installed")

    authorization_details = json.dumps(
        [
            {
                "type": "unity_catalog_privileges",
                "privileges": ["USE CATALOG"],
                "object_type": "CATALOG",
                "object_full_path": catalog,
            },
            {
                "type": "unity_catalog_privileges",
                "privileges": ["USE SCHEMA"],
                "object_type": "SCHEMA",
                "object_full_path": f"{catalog}.{schema}",
            },
            {
                "type": "unity_catalog_privileges",
                "privileges": ["SELECT", "MODIFY"],
                "object_type": "TABLE",
                "object_full_path": table_name,
            },
        ]
    )
    resp = _requests.post(
        f"{workspace_url}/oidc/v1/token",
        auth=_HTTPBasicAuth(client_id, client_secret),
        data={
            "grant_type": "client_credentials",
            "scope": "all-apis",
            "resource": f"api://databricks/workspaces/{workspace_id}/zerobusDirectWriteApi",
            "authorization_details": authorization_details,
        },
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def http_insert_url(zerobus_ingest_url: str, table_name: str) -> str:
    """Build the REST insert URL — mirrors _rest_insert_url in HTTP notebooks."""
    return f"{zerobus_ingest_url}/zerobus/v1/tables/{table_name}/insert"


# ---------------------------------------------------------------------------
# HTTP sync — 4a singles (cell 20)
# ---------------------------------------------------------------------------

def ingest_singles_http_sync(
    insert_url: str,
    token: str,
    records: list[dict],
    session: Optional[Any] = None,
) -> SinglesResult:
    """
    POST each record individually (one-element JSON array per call).

    Matches http_sync cell 20.  Pass a requests.Session for connection reuse
    across rows (eliminates repeated TCP+TLS handshakes for rows 2-N).
    send = full POST round-trip; wait = 0.
    """
    if _requests is None:
        raise ImportError("requests not installed")

    sess = session or _requests
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    row_http_seconds: list[float] = []
    bytes_4a = 0

    t_4a0 = time.perf_counter()
    for record_dict in records:
        bytes_4a += json_payload_bytes(record_dict)
        t_row = time.perf_counter()
        resp = sess.post(insert_url, headers=headers, data=json.dumps([record_dict]), timeout=120)
        resp.raise_for_status()
        row_http_seconds.append(time.perf_counter() - t_row)
    singles_wall_s = time.perf_counter() - t_4a0

    return SinglesResult(
        singles_n=len(records),
        singles_wall_s=singles_wall_s,
        row_send_seconds=list(row_http_seconds),
        row_wait_seconds=[0.0] * len(records),  # no separate ack in HTTP
        row_ack_seconds=list(row_http_seconds),
        bytes_4a=bytes_4a,
        t_4a0=t_4a0,
    )


# ---------------------------------------------------------------------------
# HTTP sync — 4b batch (cell 22)
# ---------------------------------------------------------------------------

def ingest_batch_http_sync(
    insert_url: str,
    token: str,
    records: list[dict],
    session: Optional[Any] = None,
) -> BatchResult:
    """
    POST all records as a single JSON array.

    Matches http_sync cell 22.  Pass the same requests.Session used for 4a so
    the batch reuses the warmed connection.  send = full POST; wait = 0; no stream to close.
    """
    if _requests is None:
        raise ImportError("requests not installed")

    sess = session or _requests
    batch_ack_s: Optional[float] = None
    bytes_4b = 0

    if records:
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
        bytes_4b = sum(json_payload_bytes(r) for r in records)
        t_4b0 = time.perf_counter()
        resp = sess.post(insert_url, headers=headers, data=json.dumps(records), timeout=120)
        resp.raise_for_status()
        batch_ack_s = time.perf_counter() - t_4b0

    t_after_close = time.perf_counter()

    return BatchResult(
        batch_n=len(records),
        batch_send_s=batch_ack_s,           # HTTP: send = ack (one POST)
        batch_wait_s=0.0 if batch_ack_s is not None else None,
        batch_ack_s=batch_ack_s,
        stream_close_s=None,                # no gRPC stream
        t_after_close=t_after_close,
        bytes_4b=bytes_4b,
        stream_open_s=None,                 # no gRPC stream
    )


# ---------------------------------------------------------------------------
# HTTP async — 4a singles (cell 20)
# ---------------------------------------------------------------------------

async def ingest_singles_http_async(
    insert_url: str,
    token: str,
    records: list[dict],
    session: Optional[Any] = None,
) -> SinglesResult:
    """
    Fire all single-row POSTs concurrently via asyncio.gather.

    Matches http_async cell 20.  Pass a shared aiohttp.ClientSession (created in
    cell 18) so 4a and 4b reuse the same connection pool.
    Wall time ≈ one round-trip for all rows.
    """
    if _aiohttp is None:
        raise ImportError("aiohttp not installed")

    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    bytes_4a = sum(json_payload_bytes(r) for r in records)

    async def _post_one(sess: Any, record_dict: dict) -> float:
        t0 = time.perf_counter()
        async with sess.post(
            insert_url,
            data=json.dumps([record_dict]),
            headers=headers,
            timeout=_aiohttp.ClientTimeout(total=120),
        ) as resp:
            resp.raise_for_status()
        return time.perf_counter() - t0

    own_session = session is None
    sess = _aiohttp.ClientSession() if own_session else session
    t_4a0 = time.perf_counter()
    try:
        row_http_seconds = list(
            await asyncio.gather(*[_post_one(sess, r) for r in records])
        )
    finally:
        if own_session:
            await sess.close()
    singles_wall_s = time.perf_counter() - t_4a0

    return SinglesResult(
        singles_n=len(records),
        singles_wall_s=singles_wall_s,
        row_send_seconds=list(row_http_seconds),
        row_wait_seconds=[0.0] * len(records),  # no separate ack in HTTP
        row_ack_seconds=list(row_http_seconds),
        bytes_4a=bytes_4a,
        t_4a0=t_4a0,
    )


# ---------------------------------------------------------------------------
# HTTP async — 4b batch (cell 22)
# ---------------------------------------------------------------------------

async def ingest_batch_http_async(
    insert_url: str,
    token: str,
    records: list[dict],
    session: Optional[Any] = None,
) -> BatchResult:
    """
    POST all records as a single JSON array (async — no concurrency benefit vs sync).

    Matches http_async cell 22.  Pass the shared session from cell 18 so the batch
    reuses the connection pool warmed by the 4a concurrent sends.
    """
    if _aiohttp is None:
        raise ImportError("aiohttp not installed")

    batch_ack_s: Optional[float] = None
    bytes_4b = 0

    own_session = session is None
    sess = _aiohttp.ClientSession() if own_session else session
    try:
        if records:
            headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
            bytes_4b = sum(json_payload_bytes(r) for r in records)
            t0 = time.perf_counter()
            async with sess.post(
                insert_url,
                data=json.dumps(records),
                headers=headers,
                timeout=_aiohttp.ClientTimeout(total=120),
            ) as resp:
                resp.raise_for_status()
            batch_ack_s = time.perf_counter() - t0
    finally:
        if own_session:
            await sess.close()

    t_after_close = time.perf_counter()

    return BatchResult(
        batch_n=len(records),
        batch_send_s=batch_ack_s,
        batch_wait_s=0.0 if batch_ack_s is not None else None,
        batch_ack_s=batch_ack_s,
        stream_close_s=None,
        t_after_close=t_after_close,
        bytes_4b=bytes_4b,
        stream_open_s=None,
    )


# ---------------------------------------------------------------------------
# 4c — Visibility poll (cell 24)
# ---------------------------------------------------------------------------

def poll_visibility(
    spark: Any,
    table_name: str,
    target_count: int,
    t_after_close: float,
    t_4a0: float,
    timeout: float = 120.0,
) -> VisibilityResult:
    """
    Poll COUNT(*) until target_count is reached or timeout expires, then query
    MIN/MAX bounds.

    Matches cell 24 across all four notebooks (identical code).
    t_after_close and t_4a0 come from BatchResult and SinglesResult respectively.
    """
    poll_deadline = t_after_close + timeout
    row_visible = 0
    while time.perf_counter() < poll_deadline:
        row_visible = spark.sql(f"SELECT COUNT(*) AS c FROM {table_name}").collect()[0]["c"]
        if row_visible >= target_count:
            break
        time.sleep(0.15)

    t_visible = time.perf_counter()
    bounds = spark.sql(
        f"SELECT MIN(device_name) AS lo, MAX(device_name) AS hi FROM {table_name}"
    ).collect()[0]

    return VisibilityResult(
        target_count=target_count,
        row_visible=row_visible,
        visibility_s=t_visible - t_after_close,
        visibility_from_first_send_s=t_visible - t_4a0,
        bounds_lo=bounds["lo"],
        bounds_hi=bounds["hi"],
    )


# ---------------------------------------------------------------------------
# 4d — Metrics print (cell 26)
# ---------------------------------------------------------------------------

def print_metrics(
    table_name: str,
    n: int,
    row_before: int,
    bounds_before: dict,
    singles: SinglesResult,
    batch: BatchResult,
    vis: VisibilityResult,
) -> None:
    """
    Print the Step-4d key-metrics block.

    Matches cell 26 line-for-line across all four notebooks.  stream_open_s
    and stream_close_s are taken from batch.stream_open_s / batch.stream_close_s;
    both are None for HTTP (conditional prints handle this).
    """
    ingest_4a4b_s = singles.singles_wall_s + (batch.batch_ack_s or 0.0)

    print(
        f"Before ingest: count={row_before} "
        f"min(device_name)={bounds_before['lo']!r} "
        f"max(device_name)={bounds_before['hi']!r}"
    )
    print(f"Ingested {n} rows into {table_name}")
    print(f"After ingest:  expected_count={vis.target_count} visible_count={vis.row_visible}")
    print(
        f"  visibility: {vis.visibility_from_first_send_s * 1000:.1f} ms"
        f"  (from first row send \u2192 COUNT(*) reached target)"
    )
    print(
        f"  visibility: {vis.visibility_s * 1000:.1f} ms"
        f"  (from end of ingest \u2192 COUNT(*) reached target)"
    )
    if vis.row_visible < vis.target_count:
        print("  Warning: COUNT still short after poll window; re-run or raise poll budget.")

    print(
        f"After visibility: min(device_name)={vis.bounds_lo!r} max(device_name)={vis.bounds_hi!r}"
    )

    if batch.stream_open_s is not None:
        print(f"stream open:  {batch.stream_open_s * 1000:.1f} ms")
    if batch.stream_close_s is not None:
        print(f"stream close: {batch.stream_close_s * 1000:.1f} ms")
    print(
        f"  Ingest+wait wall (4a+4b only; excludes stream.close): {ingest_4a4b_s * 1000:.1f} ms"
    )
    if singles.singles_n:
        ms_row_4a = (singles.singles_wall_s / singles.singles_n) * 1000
        print(
            f"  4a ({singles.singles_n} singles): wall {singles.singles_wall_s * 1000:.1f} ms"
            f"  (~{ms_row_4a:.1f} ms/row amortized)"
        )
        print(
            f"      per-row send:   "
            f"min={min(singles.row_send_seconds) * 1000:.1f} ms "
            f"mean={mean(singles.row_send_seconds) * 1000:.1f} ms "
            f"median={median(singles.row_send_seconds) * 1000:.1f} ms "
            f"max={max(singles.row_send_seconds) * 1000:.1f} ms"
        )
        print(
            f"      per-row wait (ack):   "
            f"min={min(singles.row_wait_seconds) * 1000:.1f} ms "
            f"mean={mean(singles.row_wait_seconds) * 1000:.1f} ms "
            f"median={median(singles.row_wait_seconds) * 1000:.1f} ms "
            f"max={max(singles.row_wait_seconds) * 1000:.1f} ms"
        )
        print(
            f"      per-row total (send+wait):   "
            f"min={min(singles.row_ack_seconds) * 1000:.1f} ms "
            f"mean={mean(singles.row_ack_seconds) * 1000:.1f} ms "
            f"median={median(singles.row_ack_seconds) * 1000:.1f} ms "
            f"max={max(singles.row_ack_seconds) * 1000:.1f} ms"
        )
    if batch.batch_ack_s is not None and batch.batch_n:
        ms_row_4b = (batch.batch_ack_s / batch.batch_n) * 1000
        print(
            f"  4b ({batch.batch_n} batched): wall {batch.batch_ack_s * 1000:.1f} ms"
            f"  (~{ms_row_4b:.3f} ms/row amortized)"
        )
        print(f"      send: {(batch.batch_send_s or 0.0) * 1000:.1f} ms")
        print(f"      wait (ack): {(batch.batch_wait_s or 0.0) * 1000:.1f} ms")
    if singles.singles_n and batch.batch_ack_s is not None and batch.batch_n:
        ms_row_4a = (singles.singles_wall_s / singles.singles_n) * 1000
        ms_row_4b = (batch.batch_ack_s / batch.batch_n) * 1000
        ratio = ms_row_4a / ms_row_4b
        print("  batch vs single latency comparison")
        print(f"    4a ({singles.singles_n} singles) {ms_row_4a:.1f} wall amortized ms/row")
        print(f"    4b (1 batch of {batch.batch_n} rows) {ms_row_4b:.3f} wall amortized ms/row")
        print(f"    4a/4b = ~{ratio:.1f}x lower ms/row")
