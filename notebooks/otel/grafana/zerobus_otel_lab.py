"""Boilerplate for ``notebooks/otel/grafana/zerobus-otel.ipynb``.

Exports OpenTelemetry via **OTLP/HTTP to Grafana Cloud** (not ZeroBus Ingest’s native OTel collector).
ZeroBus Ingest OTel (preview/Beta) exposes OTLP endpoints that land telemetry in **Unity Catalog Delta**;
this lab keeps Grafana for traces/metrics and uses standard ZeroBus ingest for table rows.
"""

from __future__ import annotations

import base64
import json
import re
import time
from contextlib import contextmanager
from urllib.parse import quote, urlparse
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterator, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, ResourceAlreadyExists, ResourceDoesNotExist
from opentelemetry import metrics, trace
from opentelemetry.instrumentation.grpc import GrpcInstrumentorClient
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.semconv.resource import ResourceAttributes
from opentelemetry.trace import Status, StatusCode


def _dbutils():
    import __main__ as main

    if hasattr(main, "dbutils"):
        return main.dbutils
    raise RuntimeError("dbutils not found — run in Databricks or Databricks Connect")


def default_otel_config(
    *,
    otel_secret_scope: str = "lfczerobusdemo",
    otel_secret_key: str = "otel-grafana-rslee6392",
) -> Dict[str, Any]:
    """Grafana OTLP lab defaults. Scope/key select the Databricks JSON merged by ``merge_grafana_otel_secret``. Use empty strings to skip secret load and set ``GRAFANA_*`` inline."""
    return {
        "DATABRICKS_OTEL_SECRET_SCOPE": otel_secret_scope,
        "DATABRICKS_OTEL_SECRET_KEY": otel_secret_key,
        "GRAFANA_OTLP_ENDPOINT": "",
        "GRAFANA_BASIC_AUTH_HEADER": "",
        "GRAFANA_INSTANCE_ID": "",
        "GRAFANA_API_TOKEN": "",
        # Browser base for deep links (not the OTLP gateway host). Example: https://myorg.grafana.net
        "GRAFANA_STACK_URL": "",
        # Tempo / traces datasource UID from Grafana → Connections → Data sources (optional; enables TraceQL link).
        "GRAFANA_TRACES_DATASOURCE_UID": "",
        "OTEL_SERVICE_NAME": "zerobus-ingest",
        "OTEL_SERVICE_VERSION": "1.0.0",
        "OTEL_ENVIRONMENT": "demo",
        "OTEL_SAMPLING_RATE": 1.0,
    }


def default_zerobus_config(
    *,
    zerobus_secret_scope: str = "lfczerobusdemo",
    zerobus_secret_key: str = "lfczerobusdemo",
) -> Dict[str, Any]:
    """ZeroBus lab defaults. Scope/key select the Databricks JSON for SP/OAuth/endpoints (defaults match ``public_example.ipynb``). Use empty strings to skip secret load/save."""
    return {
        "DATABRICKS_ZEROBUS_SECRET_SCOPE": zerobus_secret_scope,
        "DATABRICKS_ZEROBUS_SECRET_KEY": zerobus_secret_key,
        "ZEROBUS_TABLE_NAME": "air_quality_otel",
        "ZEORBUS_SCHEMA": "",
        "ZEORBUS_CATALOG": "",
        "ZEROBUS_SERVICE_PRINCIPAL_NAME": "",
        "ZEROBUS_SERVICE_PRINCIPAL_ID": "",
        "ZEROBUS_APP_ID": "",
        "ZEROBUS_OAUTH_SECRET": "",
        "DATABRICKS_WORKSPACE_URL": "",
        "ZEROBUS_SERVER_ENDPOINT": "",
        # When non-empty, used as the region label in auto-derived ZEROBUS_SERVER_ENDPOINT on every cloud.
        "DATABRICKS_REGION_OVERRIDE": "",
    }


def zerobus_sp_display_name(secret_scope: str, secret_key: str, json_field_name: str) -> str:
    """Suggested service principal ``display_name`` for ZeroBus OAuth JSON location.

    Format: ``zerobus-sp--<scope>--<secretKey>--<jsonField>`` — Databricks secret
    scope and key, then the field name inside the JSON blob (e.g. ``ZEROBUS_OAUTH_SECRET``).
    Do not use ``--`` inside scope, key, or field names (it would break parsing).
    """
    return f"zerobus-sp--{secret_scope}--{secret_key}--{json_field_name}"


_GRAFANA_OTEL_SECRET_KEYS = (
    "GRAFANA_OTLP_ENDPOINT",
    "GRAFANA_BASIC_AUTH_HEADER",
    "GRAFANA_INSTANCE_ID",
    "GRAFANA_API_TOKEN",
    "GRAFANA_STACK_URL",
    "GRAFANA_TRACES_DATASOURCE_UID",
    "OTEL_SERVICE_NAME",
    "OTEL_SERVICE_VERSION",
    "OTEL_ENVIRONMENT",
    "OTEL_SAMPLING_RATE",
)


def _otel_databricks_secret_location(otel_config: dict) -> tuple[str, str]:
    scope = (otel_config.get("DATABRICKS_OTEL_SECRET_SCOPE") or "").strip()
    key = (otel_config.get("DATABRICKS_OTEL_SECRET_KEY") or "").strip()
    return scope, key


def _ensure_otel_grafana_secret(w: WorkspaceClient, otel_config: dict) -> None:
    scope, key = _otel_databricks_secret_location(otel_config)
    if not scope or not key:
        return
    try:
        w.secrets.create_scope(scope)
        print(f"Created secret scope {scope!r}")
    except ResourceAlreadyExists:
        pass
    try:
        w.secrets.get_secret(scope, key)
    except ResourceDoesNotExist:
        template = {
            "GRAFANA_OTLP_ENDPOINT": "",
            "GRAFANA_BASIC_AUTH_HEADER": "",
            "GRAFANA_INSTANCE_ID": "",
            "GRAFANA_API_TOKEN": "",
            "GRAFANA_STACK_URL": "",
            "GRAFANA_TRACES_DATASOURCE_UID": "",
        }
        w.secrets.put_secret(
            scope=scope,
            key=key,
            string_value=json.dumps(template, indent=2),
        )
        print(
            f"Created Grafana OTEL secret {scope!r}/{key!r} — "
            "fill OTEL_CONFIG or run secrets-bootstrap.ipynb, then save_otel_grafana_to_databricks_secret()"
        )


def _load_otel_grafana_saved(otel_config: dict) -> dict:
    scope, key = _otel_databricks_secret_location(otel_config)
    if not scope or not key:
        return {}
    try:
        raw = _dbutils().secrets.get(scope=scope, key=key)
        print(f"Loaded Grafana OTEL from scope={scope!r} key={key!r}")
        return json.loads(raw)
    except Exception:
        return {}


def _apply_saved_otel_grafana(otel_config: dict, saved: dict) -> None:
    for k in _GRAFANA_OTEL_SECRET_KEYS:
        if k not in saved:
            continue
        cur = otel_config.get(k)
        if cur is None or cur == "":
            otel_config[k] = saved[k]


def merge_grafana_otel_secret(otel_config: dict) -> None:
    """Ensure scope/key exist and merge Databricks JSON into ``otel_config`` for blank fields."""
    w = WorkspaceClient()
    scope, key = _otel_databricks_secret_location(otel_config)
    if scope and key:
        _ensure_otel_grafana_secret(w, otel_config)
        _apply_saved_otel_grafana(otel_config, _load_otel_grafana_saved(otel_config))
    else:
        print(
            "Skipping Grafana OTEL secret load: set DATABRICKS_OTEL_SECRET_SCOPE and "
            "DATABRICKS_OTEL_SECRET_KEY (both non-empty), or fill GRAFANA_* inline."
        )


def save_otel_grafana_to_databricks_secret(otel_config: dict) -> None:
    scope, key = _otel_databricks_secret_location(otel_config)
    if not scope or not key:
        raise ValueError(
            "Set otel_config['DATABRICKS_OTEL_SECRET_SCOPE'] and ['DATABRICKS_OTEL_SECRET_KEY'] before saving."
        )
    w = WorkspaceClient()
    payload = {k: otel_config[k] for k in _GRAFANA_OTEL_SECRET_KEYS if k in otel_config}
    try:
        current = json.loads(_dbutils().secrets.get(scope=scope, key=key))
    except Exception:
        current = {}
    current.update(payload)
    w.secrets.put_secret(
        scope=scope,
        key=key,
        string_value=json.dumps(current, indent=2),
    )
    print(f"Saved to {scope}/{key}: {list(payload.keys())}")


@dataclass
class OtelTelemetry:
    otel_config: Dict[str, Any]
    trace_provider: TracerProvider
    meter_provider: MeterProvider
    tracer: Any
    meter: Any
    trace_operation: Callable
    record_metrics: Callable


def _otlp_http_trace_metric_endpoints(otlp_base: str) -> tuple[str, str]:
    """Derive OTLP/HTTP exporter URLs from a configured base/host.

    Grafana Cloud gateways expect ``https://<host>/otlp/v1/traces`` (404 if ``/v1/traces`` is used at host root).
    If the base already ends with ``/otlp`` or a full ``.../v1/traces`` URL, paths are left consistent.
    """
    base = (otlp_base or "").strip().rstrip("/")
    if not base:
        raise ValueError("empty OTLP base")

    lower = base.lower()
    if lower.endswith("/v1/traces"):
        trace_ep = base
        metric_ep = base[: -len("traces")] + "metrics"
        return trace_ep, metric_ep

    if lower.endswith("/otlp"):
        return f"{base}/v1/traces", f"{base}/v1/metrics"

    parsed = urlparse(base)
    path = parsed.path or ""
    host = (parsed.hostname or "").lower()
    if host.endswith(".grafana.net") and "/otlp" not in path:
        prefix = f"{base}/otlp"
        return f"{prefix}/v1/traces", f"{prefix}/v1/metrics"

    return f"{base}/v1/traces", f"{base}/v1/metrics"


def _grafana_explore_traces_url(otel_config: dict) -> Optional[str]:
    """HTTPS Explore link for traces (same spirit as the Databricks ``Table:`` URL).

    Set ``GRAFANA_STACK_URL`` to your Grafana UI origin (e.g. ``https://myorg.grafana.net``), *not* the
    OTLP gateway host. ``GRAFANA_TRACES_DATASOURCE_UID`` is the **UID** from the traces datasource
    **Settings** tab (often ``grafanacloud-<stack>-traces``), *not* the datasource **HTTP URL*
    (e.g. ``https://tempo-prod-….grafana.net/tempo``).
    """
    stack = (otel_config.get("GRAFANA_STACK_URL") or "").strip().rstrip("/")
    if not stack:
        return None

    uid = (otel_config.get("GRAFANA_TRACES_DATASOURCE_UID") or "").strip()
    svc = (otel_config.get("OTEL_SERVICE_NAME") or "zerobus-ingest").strip() or "zerobus-ingest"
    safe = svc.replace("\\", "\\\\").replace('"', '\\"')

    if uid:
        left = {
            "datasource": uid,
            "queries": [
                {
                    "refId": "A",
                    "datasource": {"type": "tempo", "uid": uid},
                    "queryType": "traceql",
                    "query": f'{{ resource.service.name = "{safe}" }}',
                }
            ],
            "range": {"from": "now-1h", "to": "now"},
        }
        payload = quote(json.dumps(left, separators=(",", ":")))
        return f"{stack}/explore?orgId=1&left={payload}"

    return f"{stack}/explore"


def create_otel_telemetry(otel_config: dict, zerobus_config: dict) -> OtelTelemetry:
    workspace_url = zerobus_config.get("DATABRICKS_WORKSPACE_URL") or "unknown"

    resource = Resource.create(
        {
            ResourceAttributes.SERVICE_NAME: otel_config["OTEL_SERVICE_NAME"],
            ResourceAttributes.SERVICE_VERSION: otel_config["OTEL_SERVICE_VERSION"],
            "environment": otel_config["OTEL_ENVIRONMENT"],
            "databricks.workspace": workspace_url,
        }
    )

    trace_provider = TracerProvider(resource=resource)

    from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

    base = (otel_config.get("GRAFANA_OTLP_ENDPOINT") or "").rstrip("/")
    auth_header = (otel_config.get("GRAFANA_BASIC_AUTH_HEADER") or "").strip()
    iid = (otel_config.get("GRAFANA_INSTANCE_ID") or "").strip()
    token = (otel_config.get("GRAFANA_API_TOKEN") or "").strip()

    if not base:
        raise ValueError(
            "Grafana OTLP needs GRAFANA_OTLP_ENDPOINT (from Grafana UI or Databricks secret). "
            "Set DATABRICKS_OTEL_SECRET_SCOPE + DATABRICKS_OTEL_SECRET_KEY and merge_grafana_otel_secret(), "
            "or set GRAFANA_* inline / see secrets-bootstrap.ipynb."
        )

    # Prefer instance id + token to build Basic auth when both are set. Grafana's precomputed
    # GRAFANA_BASIC_AUTH_HEADER in the secret can go stale after token rotation while id+token stay updated.
    if iid and token:
        b64 = base64.b64encode(f"{iid}:{token}".encode()).decode("ascii")
        headers = {"Authorization": f"Basic {b64}"}
        print("📊 OTEL: Grafana Authorization from GRAFANA_INSTANCE_ID + GRAFANA_API_TOKEN (Basic)")
    elif auth_header:
        low = auth_header.lower()
        if low.startswith("basic "):
            headers = {"Authorization": auth_header}
        else:
            headers = {"Authorization": f"Basic {auth_header}"}
        print("📊 OTEL: Grafana Authorization from GRAFANA_BASIC_AUTH_HEADER")
    else:
        raise ValueError(
            "Grafana OTLP needs GRAFANA_BASIC_AUTH_HEADER or both GRAFANA_INSTANCE_ID and GRAFANA_API_TOKEN "
            "(from Databricks secret or inline)."
        )

    trace_ep, metric_ep = _otlp_http_trace_metric_endpoints(base)

    span_exporter = OTLPSpanExporter(endpoint=trace_ep, headers=headers)
    metric_exporter = OTLPMetricExporter(endpoint=metric_ep, headers=headers)
    print(f"📊 OTEL: Grafana OTLP HTTP traces={trace_ep} metrics={metric_ep}")

    trace_provider.add_span_processor(BatchSpanProcessor(span_exporter))
    trace.set_tracer_provider(trace_provider)

    metric_reader = PeriodicExportingMetricReader(metric_exporter, export_interval_millis=5000)
    meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
    metrics.set_meter_provider(meter_provider)

    GrpcInstrumentorClient().instrument()
    RequestsInstrumentor().instrument()

    tracer = trace.get_tracer(__name__)
    meter = metrics.get_meter(__name__)
    records_counter = meter.create_counter(
        name="zerobus.records.ingested",
        description="Number of records ingested",
        unit="records",
    )
    bytes_counter = meter.create_counter(
        name="zerobus.bytes.ingested",
        description="Number of bytes ingested",
        unit="bytes",
    )
    ingest_duration = meter.create_histogram(
        name="zerobus.ingest.duration",
        description="Duration of ingest operations",
        unit="ms",
    )

    @contextmanager
    def trace_operation(name: str, attributes: Optional[dict] = None) -> Iterator[Any]:
        with tracer.start_as_current_span(name) as span:
            if attributes:
                for key, value in attributes.items():
                    span.set_attribute(key, value)
            start_time = time.time()
            try:
                yield span
                span.set_status(Status(StatusCode.OK))
            except Exception as e:
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.record_exception(e)
                raise
            finally:
                span.set_attribute("duration_ms", (time.time() - start_time) * 1000)

    def record_metrics(
        operation: str, records: int = 0, bytes_size: int = 0, duration_ms: float = 0
    ) -> None:
        labels = {"operation": operation, "table": zerobus_config.get("ZEROBUS_TABLE_NAME", "unknown")}
        if records > 0:
            records_counter.add(records, labels)
        if bytes_size > 0:
            bytes_counter.add(bytes_size, labels)
        if duration_ms > 0:
            ingest_duration.record(duration_ms, labels)

    print("✅ OpenTelemetry initialized successfully!")
    return OtelTelemetry(
        otel_config=otel_config,
        trace_provider=trace_provider,
        meter_provider=meter_provider,
        tracer=tracer,
        meter=meter,
        trace_operation=trace_operation,
        record_metrics=record_metrics,
    )


@dataclass
class ZerobusOtelContext:
    w: WorkspaceClient
    username: str
    config: Dict[str, Any]
    config_original: Dict[str, Any]
    table_catalog: str
    table_schema: str
    table_name: str
    fq_table_name: str
    table_url: str
    telemetry: OtelTelemetry

    def save_config_if_changed(self) -> None:
        changed = {k: v for k, v in self.config.items() if self.config_original.get(k) != v}
        changed = {k: v for k, v in changed.items() if k not in _ZB_SECRET_LOCATION_KEYS}
        if changed:
            _save_zerobus_config(changed, self.w, self.config)
        else:
            print("Config unchanged — nothing to save")


_ZB_SECRET_LOCATION_KEYS = frozenset(
    ("DATABRICKS_ZEROBUS_SECRET_SCOPE", "DATABRICKS_ZEROBUS_SECRET_KEY")
)


def _zb_secret_location(cfg: dict) -> tuple[str, str]:
    scope = (cfg.get("DATABRICKS_ZEROBUS_SECRET_SCOPE") or "").strip()
    key = (cfg.get("DATABRICKS_ZEROBUS_SECRET_KEY") or "").strip()
    return scope, key


def _ensure_zb_secret_scope_and_key(w: WorkspaceClient, cfg: dict) -> None:
    scope, key = _zb_secret_location(cfg)
    if not scope or not key:
        return
    try:
        w.secrets.create_scope(scope)
        print(f"Created secret scope {scope!r}")
    except ResourceAlreadyExists:
        pass
    try:
        w.secrets.get_secret(scope, key)
    except ResourceDoesNotExist:
        w.secrets.put_secret(scope=scope, key=key, string_value="{}")
        print(f"Created empty secret {scope!r}/{key!r}")


def _load_zb_saved(w: WorkspaceClient, cfg: dict) -> dict:
    scope, key = _zb_secret_location(cfg)
    if not scope or not key:
        print(
            "Skipping ZeroBus secret load: set DATABRICKS_ZEROBUS_SECRET_SCOPE and "
            "DATABRICKS_ZEROBUS_SECRET_KEY (both non-empty), or set ZEROBUS_* / endpoints inline."
        )
        return {}
    try:
        raw = _dbutils().secrets.get(scope=scope, key=key)
        print(f"Loaded config from secret scope={scope!r} key={key!r}")
        return json.loads(raw)
    except Exception:
        pass
    _ensure_zb_secret_scope_and_key(w, cfg)
    return {}


def _merge_zb_config(defaults: dict, w: WorkspaceClient) -> tuple[dict, dict]:
    saved = _load_zb_saved(w, defaults)
    saved_clean = {k: v for k, v in saved.items() if k not in _ZB_SECRET_LOCATION_KEYS}
    merged = {k: v if v else saved_clean.get(k, v) for k, v in defaults.items()} | {
        k: v for k, v in saved_clean.items() if k not in defaults
    }
    return merged, dict(saved_clean)


def _save_zerobus_config(updates: dict, w: WorkspaceClient, cfg: dict) -> bool:
    scope, key = _zb_secret_location(cfg)
    if not scope or not key:
        print(
            "Skipping ZeroBus secret save: set DATABRICKS_ZEROBUS_SECRET_SCOPE and "
            "DATABRICKS_ZEROBUS_SECRET_KEY (both non-empty)."
        )
        return False
    updates = {k: v for k, v in updates.items() if k not in _ZB_SECRET_LOCATION_KEYS}
    if not updates:
        return True
    try:
        current = json.loads(_dbutils().secrets.get(scope=scope, key=key))
    except Exception:
        current = {}
    current.update(updates)
    w.secrets.put_secret(
        scope=scope,
        key=key,
        string_value=json.dumps(current, indent=2),
    )
    print(f"Saved {list(updates.keys())} to secret {scope}/{key}")
    return True


def bootstrap_zerobus_with_otel(
    telemetry: OtelTelemetry,
    zerobus_config: Dict[str, Any],
    spark: Any,
) -> ZerobusOtelContext:
    """Workspace client, secrets merge, endpoints, SP, OAuth, table DDL, grants — mutates ``zerobus_config``."""
    trace_operation = telemetry.trace_operation
    cfg = zerobus_config

    with trace_operation("workspace_setup", {"step": "initialization"}):
        w = WorkspaceClient()
        username = re.sub(r"[^a-z0-9]", "_", w.current_user.me().user_name.split("@")[0])

    merged, saved_snapshot = _merge_zb_config(cfg, w)
    cfg.clear()
    cfg.update(merged)

    def get_endpoint_workspace_url(SERVER_ENDPOINT: str, DATABRICKS_WORKSPACE_URL: str):
        with trace_operation("endpoint_discovery") as span:
            generated = False
            _url_mismatch = DATABRICKS_WORKSPACE_URL and DATABRICKS_WORKSPACE_URL != w.config.host
            if _url_mismatch:
                span.add_event(
                    "URL mismatch detected",
                    {"provided": DATABRICKS_WORKSPACE_URL, "actual": w.config.host},
                )
                print(
                    f"Warning: DATABRICKS_WORKSPACE_URL={DATABRICKS_WORKSPACE_URL!r} does not match "
                    f"WorkspaceClient host={w.config.host!r}. Recalculating both values."
                )
                SERVER_ENDPOINT = ""
                DATABRICKS_WORKSPACE_URL = ""

            if not SERVER_ENDPOINT or not DATABRICKS_WORKSPACE_URL:
                if not DATABRICKS_WORKSPACE_URL:
                    DATABRICKS_WORKSPACE_URL = w.config.host
                if not SERVER_ENDPOINT:
                    _workspace_id = w.get_workspace_id()
                    _region_override = (
                        (cfg.get("DATABRICKS_REGION_OVERRIDE") or cfg.get("DATABRICKS_REGION_GCP_OVERRIDE") or "")
                    ).strip()
                    if _region_override:
                        _region = _region_override
                        print(
                            "Using DATABRICKS_REGION_OVERRIDE for ZeroBus endpoint region "
                            f"(all clouds): {_region!r}"
                        )
                    elif "azuredatabricks.net" in DATABRICKS_WORKSPACE_URL:
                        _region = spark.sql("SELECT current_metastore()").collect()[0][0].split(":")[1]
                    else:
                        _region = w.clusters.list_zones().default_zone[:-1]
                    _domain = (
                        "azuredatabricks.net"
                        if "azuredatabricks.net" in DATABRICKS_WORKSPACE_URL
                        else "cloud.databricks.com"
                    )
                    SERVER_ENDPOINT = f"https://{_workspace_id}.zerobus.{_region}.{_domain}"
                generated = True
                span.set_attribute("generated", True)
                span.set_attribute("endpoint", SERVER_ENDPOINT)
                span.set_attribute("workspace_url", DATABRICKS_WORKSPACE_URL)
                print(f"Auto-derived SERVER_ENDPOINT={SERVER_ENDPOINT}")
                print(f"Auto-derived DATABRICKS_WORKSPACE_URL={DATABRICKS_WORKSPACE_URL}")

            return SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL, generated

    cfg["ZEROBUS_SERVER_ENDPOINT"], cfg["DATABRICKS_WORKSPACE_URL"], _gen = get_endpoint_workspace_url(
        cfg.get("ZEROBUS_SERVER_ENDPOINT", ""),
        cfg.get("DATABRICKS_WORKSPACE_URL", ""),
    )
    print(f"{cfg['ZEROBUS_SERVER_ENDPOINT']=}")
    print(f"{cfg['DATABRICKS_WORKSPACE_URL']=}")

    def _ensure_sp() -> None:
        with trace_operation("service_principal_setup") as span:
            sp_id = cfg.get("ZEROBUS_SERVICE_PRINCIPAL_ID", "")

            def _create_sp():
                _name = (
                    cfg["ZEROBUS_SERVICE_PRINCIPAL_NAME"]
                    if cfg["ZEROBUS_SERVICE_PRINCIPAL_NAME"]
                    else zerobus_sp_display_name(
                        (cfg.get("DATABRICKS_ZEROBUS_SECRET_SCOPE") or "lfczerobusdemo").strip(),
                        (cfg.get("DATABRICKS_ZEROBUS_SECRET_KEY") or "lfczerobusdemo").strip(),
                        "ZEROBUS_OAUTH_SECRET",
                    )
                )
                cfg["ZEROBUS_SERVICE_PRINCIPAL_NAME"] = _name
                _sp = w.service_principals.create(display_name=_name)
                cfg["ZEROBUS_SERVICE_PRINCIPAL_ID"] = str(_sp.id)
                cfg["ZEROBUS_APP_ID"] = str(_sp.application_id)
                span.set_attribute("sp_created", True)
                span.set_attribute("sp_name", _name)
                span.set_attribute("sp_id", str(_sp.id))
                print(f"Created SP '{_name}' id={_sp.id} APP_ID={cfg['ZEROBUS_APP_ID']}")

            if not sp_id:
                print("ZEROBUS_SERVICE_PRINCIPAL_ID not set — creating service principal")
                _create_sp()
            else:
                try:
                    _sp = w.service_principals.get(sp_id)
                    span.set_attribute("sp_exists", True)
                    span.set_attribute("sp_id", sp_id)
                    print(f"SP exists: '{_sp.display_name}'  id={sp_id}")
                    if not cfg.get("ZEROBUS_APP_ID"):
                        cfg["ZEROBUS_APP_ID"] = str(_sp.application_id)
                        _save_zerobus_config({"ZEROBUS_APP_ID": cfg["ZEROBUS_APP_ID"]}, w, cfg)
                        print(f"Backfilled ZEROBUS_APP_ID={cfg['ZEROBUS_APP_ID']}")
                except NotFound:
                    span.add_event("SP not found, creating replacement")
                    print(f"SP id={sp_id} not found — creating a replacement")
                    _create_sp()

    _ensure_sp()
    print(f"ZEROBUS_SERVICE_PRINCIPAL_ID = {cfg['ZEROBUS_SERVICE_PRINCIPAL_ID']}")
    print(f"ZEROBUS_APP_ID               = {cfg['ZEROBUS_APP_ID']}")

    def _credentials_valid(client_id: str, client_secret: str, DATABRICKS_WORKSPACE_URL: str) -> bool:
        with trace_operation("credential_validation", {"client_id": client_id}) as span:
            if not client_id or not client_secret:
                span.set_attribute("valid", False)
                span.set_attribute("reason", "missing_credentials")
                return False
            try:
                import requests as _req

                _resp = _req.post(
                    f"{DATABRICKS_WORKSPACE_URL.rstrip('/')}/oidc/v1/token",
                    data={
                        "grant_type": "client_credentials",
                        "client_id": client_id,
                        "client_secret": client_secret,
                        "scope": "all-apis",
                    },
                    timeout=10,
                )
                span.set_attribute("status_code", _resp.status_code)
                span.set_attribute("valid", _resp.ok)
                if not _resp.ok:
                    span.add_event(
                        "Validation failed", {"status": _resp.status_code, "error": _resp.text[:200]}
                    )
                    print(f"Exception: {_resp.status_code} {_resp.text}")
                return _resp.ok
            except Exception as ex:
                span.record_exception(ex)
                span.set_attribute("valid", False)
                print(f"Exception: {ex}")
                return False

    client_and_secret_valid = _credentials_valid(
        cfg["ZEROBUS_APP_ID"],
        cfg["ZEROBUS_OAUTH_SECRET"],
        cfg["DATABRICKS_WORKSPACE_URL"],
    )

    if client_and_secret_valid:
        print("client (app) secret is valid")
    else:
        with trace_operation("create_oauth_secret") as span:
            print("client (app) secret is not valid")
            _sp_id = cfg["ZEROBUS_SERVICE_PRINCIPAL_ID"]
            _secret_obj = None
            try:
                _secret_obj = w.service_principal_secrets_proxy.create(service_principal_id=_sp_id)
            except AttributeError:
                try:
                    from databricks.sdk import ServicePrincipalSecretsAPI

                    _secret_obj = ServicePrincipalSecretsAPI(w.api_client).create(
                        service_principal_id=_sp_id
                    )
                except Exception:
                    _resp = w.api_client.do(
                        "POST", f"/api/2.0/accounts/servicePrincipals/{_sp_id}/credentials/secrets"
                    )
                    cfg["ZEROBUS_OAUTH_SECRET"] = _resp["secret"]

            if _secret_obj is not None:
                cfg["ZEROBUS_OAUTH_SECRET"] = _secret_obj.secret
            if not _save_zerobus_config({"ZEROBUS_OAUTH_SECRET": cfg["ZEROBUS_OAUTH_SECRET"]}, w, cfg):
                print(
                    "WARNING: ZEROBUS_OAUTH_SECRET was created in this session but not persisted — "
                    "set DATABRICKS_ZEROBUS_SECRET_SCOPE and DATABRICKS_ZEROBUS_SECRET_KEY."
                )
            span.set_attribute("secret_created", True)

    with trace_operation("table_setup") as span:
        fq_table_name_parts = cfg["ZEROBUS_TABLE_NAME"].split(".")
        table_catalog = "main" if len(fq_table_name_parts) <= 2 else fq_table_name_parts.pop(0)
        table_schema = username if len(fq_table_name_parts) < 2 else fq_table_name_parts.pop(0)
        table_name = (
            fq_table_name_parts.pop(0) if len(fq_table_name_parts) > 0 else "air_quality_otel"
        )
        fq_table_name = f"{table_catalog}.{table_schema}.{table_name}"

        span.set_attribute("catalog", table_catalog)
        span.set_attribute("schema", table_schema)
        span.set_attribute("table", table_name)
        span.set_attribute("fq_table_name", fq_table_name)

        if not table_name:
            raise RuntimeError("table not specified")

        print(f"{table_catalog=} {table_schema=} {table_name=}")

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {table_catalog}.{table_schema}")

        _expected = {"device_name", "temp", "humidity"}
        try:
            _actual = {f.name for f in spark.table(fq_table_name).schema}
            if not _expected.issubset(_actual):
                span.add_event("Schema mismatch, recreating table")
                print(f"Schema mismatch (found {_actual}), dropping and recreating...")
                spark.sql(f"DROP TABLE IF EXISTS {fq_table_name}")
        except Exception:
            pass

        spark.sql(
            f"""
    CREATE TABLE IF NOT EXISTS {fq_table_name} (
      device_name STRING,
      temp        INT,
      humidity    INT
    )
    """
        )

    def _has_grant(principal: str, action: str, securable_type: str, securable_name: str) -> bool:
        rows = spark.sql(f"SHOW GRANTS `{principal}` ON {securable_type} {securable_name}").collect()
        return any(r["ActionType"] == action for r in rows)

    with trace_operation("grant_permissions") as span:
        grants_added = []
        if not _has_grant(cfg["ZEROBUS_APP_ID"], "USE CATALOG", "CATALOG", table_catalog):
            spark.sql(f"GRANT USE CATALOG ON CATALOG {table_catalog} TO `{cfg['ZEROBUS_APP_ID']}`")
            grants_added.append(f"USE CATALOG on {table_catalog}")
            print(f"WARNING: Granted USE CATALOG on {table_catalog}")
        else:
            print(f"USE CATALOG on {table_catalog} already granted")

        if not _has_grant(
            cfg["ZEROBUS_APP_ID"], "USE SCHEMA", "SCHEMA", f"{table_catalog}.{table_schema}"
        ):
            spark.sql(
                f"GRANT USE SCHEMA ON SCHEMA {table_catalog}.{table_schema} TO `{cfg['ZEROBUS_APP_ID']}`"
            )
            grants_added.append(f"USE SCHEMA on {table_catalog}.{table_schema}")
            print(f"WARNING: Granted USE SCHEMA on {table_catalog}.{table_schema}")
        else:
            print(f"USE SCHEMA on {table_catalog}.{table_schema} already granted")

        for action in ["MODIFY", "SELECT"]:
            if not _has_grant(cfg["ZEROBUS_APP_ID"], action, "TABLE", fq_table_name):
                spark.sql(f"GRANT {action} ON TABLE {fq_table_name} TO `{cfg['ZEROBUS_APP_ID']}`")
                grants_added.append(f"{action} on {fq_table_name}")
                print(f"WARNING: Granted {action} on {fq_table_name}")
            else:
                print(f"{action} on {fq_table_name} already granted")

        span.set_attribute("grants_added", len(grants_added))
        if grants_added:
            span.add_event("Permissions granted", {"grants": ", ".join(grants_added)})

    table_url = (
        f"{cfg['DATABRICKS_WORKSPACE_URL'].rstrip('/')}/explore/data/"
        f"{table_catalog}/{table_schema}/{table_name}"
    )
    print(f"Table: {table_url}")

    _grafana_traces = _grafana_explore_traces_url(telemetry.otel_config)
    if _grafana_traces:
        print(f"Grafana traces: {_grafana_traces}")

    _detail = spark.sql(f"DESCRIBE DETAIL {fq_table_name}").collect()[0]
    _location = _detail["location"]
    _rejected_path = f"{_location}/_zerobus/table_rejected_parquets/"
    print(f"Table storage : {_location}")
    print(f"Rejected rows : {_rejected_path}")

    return ZerobusOtelContext(
        w=w,
        username=username,
        config=cfg,
        config_original=saved_snapshot,
        table_catalog=table_catalog,
        table_schema=table_schema,
        table_name=table_name,
        fq_table_name=fq_table_name,
        table_url=table_url,
        telemetry=telemetry,
    )


def flush_telemetry_and_save(ctx: ZerobusOtelContext) -> None:
    """Flush OTEL providers and persist Zerobus config if changed."""
    oc = ctx.telemetry.otel_config
    svc = (oc.get("OTEL_SERVICE_NAME") or "unknown").strip() or "unknown"
    env = (oc.get("OTEL_ENVIRONMENT") or "").strip()

    print("\n📊 Flushing remaining telemetry...")
    ctx.telemetry.trace_provider.force_flush()
    ctx.telemetry.meter_provider.force_flush()
    ctx.save_config_if_changed()
    print("\n✅ OpenTelemetry instrumentation complete!")

    _grafana_traces = _grafana_explore_traces_url(oc)
    if _grafana_traces:
        print(f"Grafana traces: {_grafana_traces}")
        print(
            "Traces: **Table** (list) can show rows even when the TraceQL panel looks empty — use the table/list "
            "icon. Open **zerobus_ingestion** (~1s); nested **GET** spans are **requests**/gRPC auto-instrumentation "
            "on outbound HTTP, not missing ZeroBus spans."
        )
        print(
            "Metrics: OTLP **metrics** go to **Grafana Cloud Metrics** (Mimir), not the Tempo traces UI. "
            "Use **Explore → Metrics** and select your **Prometheus** datasource (often **grafanacloud-*-prom**), "
            'not **grafanacloud-*-traces**. Try PromQL {__name__=~".*zerobus.*"} or search **zerobus**. '
            "This notebook emits **zerobus.records.ingested**, **zerobus.bytes.ingested**, **zerobus.ingest.duration** "
            "(names in the UI may use **underscores** instead of dots). **Run the flush cell** so counters export; allow a short delay."
        )
    else:
        print("\n📈 Grafana: set **GRAFANA_STACK_URL** in OTEL_CONFIG or the Grafana Databricks secret")
        print("   (your UI origin, e.g. https://myorg.grafana.net — not the OTLP gateway host) to print a")
        print("   **Grafana traces:** URL like **Table:**. Optional **GRAFANA_TRACES_DATASOURCE_UID** adds a TraceQL filter.")
        print(f"\n   Until then: Explore → Traces / Tempo → service **{svc!r}** → span names like **zerobus_ingestion**.")
        if env:
            print(f"   Resource **environment** = **{env!r}**.")
