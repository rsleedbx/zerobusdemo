"""OpenTelemetry → Databricks workspace OTLP → Unity Catalog Delta (traces/metrics).

Requires ``zerobus_otel_lab`` importable (same repo): reuses ``bootstrap_zerobus_with_otel`` and ``OtelTelemetry``.
See ``docs/zerobus-ingest-otel-uc.md`` for DDL, token grants, and headers. This demo exports **traces + metrics**
(http/protobuf); **logs** use the same endpoint/header pattern in the doc.

Run with ``notebooks/otel/grafana`` on ``sys.path`` before this package so ``import zerobus_otel_lab`` works.
"""

from __future__ import annotations

import json
import re
import time
from contextlib import contextmanager
from typing import Any, Callable, Dict, Iterator, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceAlreadyExists, ResourceDoesNotExist
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

from zerobus_otel_lab import OtelTelemetry, ZerobusOtelContext, bootstrap_zerobus_with_otel


def _dbutils():
    import __main__ as main

    if hasattr(main, "dbutils"):
        return main.dbutils
    raise RuntimeError("dbutils not found — run in Databricks or Databricks Connect")


def default_uc_otel_config(
    *,
    uc_otel_secret_scope: str = "lfczerobusdemo",
    uc_otel_secret_key: str = "uc_otel_config",
) -> Dict[str, Any]:
    return {
        "DATABRICKS_UC_OTEL_SECRET_SCOPE": uc_otel_secret_scope,
        "DATABRICKS_UC_OTEL_SECRET_KEY": uc_otel_secret_key,
        "DATABRICKS_WORKSPACE_URL": "",
        "UC_OTEL_CATALOG": "main",
        "UC_OTEL_SCHEMA": "",
        "UC_OTEL_PREFIX": "zerobus_uc_demo",
        "DATABRICKS_TOKEN": "",
        "OTEL_SERVICE_NAME": "zerobus-ingest-uc",
        "OTEL_SERVICE_VERSION": "1.0.0",
        "OTEL_ENVIRONMENT": "demo",
    }


_UC_OTEL_SECRET_KEYS = (
    "DATABRICKS_WORKSPACE_URL",
    "UC_OTEL_CATALOG",
    "UC_OTEL_SCHEMA",
    "UC_OTEL_PREFIX",
    "DATABRICKS_TOKEN",
    "OTEL_SERVICE_NAME",
    "OTEL_SERVICE_VERSION",
    "OTEL_ENVIRONMENT",
)


def _uc_otel_secret_location(cfg: dict) -> tuple[str, str]:
    scope = (cfg.get("DATABRICKS_UC_OTEL_SECRET_SCOPE") or "").strip()
    key = (cfg.get("DATABRICKS_UC_OTEL_SECRET_KEY") or "").strip()
    return scope, key


def _ensure_uc_otel_secret_scope_and_key(w: WorkspaceClient, cfg: dict) -> None:
    scope, key = _uc_otel_secret_location(cfg)
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
        template = {k: "" for k in _UC_OTEL_SECRET_KEYS}
        w.secrets.put_secret(scope=scope, key=key, string_value=json.dumps(template, indent=2))
        print(f"Created empty secret {scope!r}/{key!r} — fill UC OTEL JSON and re-run merge")


def _load_uc_otel_saved(cfg: dict) -> dict:
    scope, key = _uc_otel_secret_location(cfg)
    if not scope or not key:
        print(
            "Skipping UC OTEL secret load: set DATABRICKS_UC_OTEL_SECRET_SCOPE and "
            "DATABRICKS_UC_OTEL_SECRET_KEY, or set workspace / UC fields inline."
        )
        return {}
    try:
        raw = _dbutils().secrets.get(scope=scope, key=key)
        print(f"Loaded UC OTEL from scope={scope!r} key={key!r}")
        return json.loads(raw)
    except Exception:
        pass
    w = WorkspaceClient()
    _ensure_uc_otel_secret_scope_and_key(w, cfg)
    return {}


def merge_uc_otel_secret(uc_otel_config: dict) -> None:
    """Merge Databricks JSON secret into ``uc_otel_config`` for blank fields."""
    saved = _load_uc_otel_saved(uc_otel_config)
    for k in _UC_OTEL_SECRET_KEYS:
        if k not in saved:
            continue
        cur = uc_otel_config.get(k)
        if cur is None or cur == "":
            uc_otel_config[k] = saved[k]


def resolve_uc_otel_schema_if_blank(uc_otel_config: dict, spark: Any) -> None:
    """If ``UC_OTEL_SCHEMA`` is empty, set it from ``current_user()`` (sanitized), like ``public_example``."""
    if (uc_otel_config.get("UC_OTEL_SCHEMA") or "").strip():
        return
    user = spark.sql("SELECT current_user()").collect()[0][0]
    uc_otel_config["UC_OTEL_SCHEMA"] = re.sub(r"[^a-z0-9]", "_", user.split("@")[0].lower())
    print(f"UC_OTEL_SCHEMA default: {uc_otel_config['UC_OTEL_SCHEMA']!r}")


def uc_otel_table_names(catalog: str, schema: str, prefix: str) -> Dict[str, str]:
    p = prefix.strip()
    base = f"{catalog.strip()}.{schema.strip()}.{p}"
    return {
        "spans": f"{base}_otel_spans",
        "logs": f"{base}_otel_logs",
        "metrics": f"{base}_otel_metrics",
    }


def assert_uc_otel_tables_exist(spark: Any, catalog: str, schema: str, prefix: str) -> None:
    names = uc_otel_table_names(catalog, schema, prefix)
    missing = [fqn for fqn in names.values() if not spark.catalog.tableExists(fqn)]
    if missing:
        raise RuntimeError(
            "Missing UC OTEL Delta table(s): "
            + ", ".join(missing)
            + ". Create them with PrD/PrPr DDL (see docs/zerobus-ingest-otel-uc.md) and grant MODIFY/SELECT."
        )
    print("UC OTEL tables present:", ", ".join(names.values()))


def _workspace_base(url: str) -> str:
    return (url or "").strip().rstrip("/")


def create_databricks_uc_otel_telemetry(
    uc_otel_config: dict, zerobus_config: dict, spark: Any
) -> OtelTelemetry:
    """OTLP/http/protobuf to ``/api/2.0/otel/v1/*`` with UC table headers (traces + metrics)."""
    resolve_uc_otel_schema_if_blank(uc_otel_config, spark)

    workspace = _workspace_base(uc_otel_config.get("DATABRICKS_WORKSPACE_URL") or "")
    token = (uc_otel_config.get("DATABRICKS_TOKEN") or "").strip()
    catalog = (uc_otel_config.get("UC_OTEL_CATALOG") or "").strip()
    schema = (uc_otel_config.get("UC_OTEL_SCHEMA") or "").strip()
    prefix = (uc_otel_config.get("UC_OTEL_PREFIX") or "").strip()

    if not workspace:
        raise ValueError(
            "Set DATABRICKS_WORKSPACE_URL (Databricks workspace origin, e.g. https://….cloud.databricks.com)."
        )
    if not token:
        raise ValueError("Set DATABRICKS_TOKEN (PAT or SP token with MODIFY on the three OTEL tables).")
    if not catalog or not schema or not prefix:
        raise ValueError("Set UC_OTEL_CATALOG, UC_OTEL_SCHEMA, and UC_OTEL_PREFIX.")

    assert_uc_otel_tables_exist(spark, catalog, schema, prefix)
    tables = uc_otel_table_names(catalog, schema, prefix)

    trace_ep = f"{workspace}/api/2.0/otel/v1/traces"
    metric_ep = f"{workspace}/api/2.0/otel/v1/metrics"

    common_headers = {
        "content-type": "application/x-protobuf",
        "Authorization": f"Bearer {token}",
    }

    from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

    span_exporter = OTLPSpanExporter(
        endpoint=trace_ep,
        headers={**common_headers, "X-Databricks-UC-Table-Name": tables["spans"]},
    )
    metric_exporter = OTLPMetricExporter(
        endpoint=metric_ep,
        headers={**common_headers, "X-Databricks-UC-Table-Name": tables["metrics"]},
    )

    zb_ws = zerobus_config.get("DATABRICKS_WORKSPACE_URL") or workspace
    resource = Resource.create(
        {
            ResourceAttributes.SERVICE_NAME: uc_otel_config["OTEL_SERVICE_NAME"],
            ResourceAttributes.SERVICE_VERSION: uc_otel_config["OTEL_SERVICE_VERSION"],
            "environment": uc_otel_config["OTEL_ENVIRONMENT"],
            "databricks.workspace": zb_ws,
            "databricks.uc.otel.spans_table": tables["spans"],
            "databricks.uc.otel.metrics_table": tables["metrics"],
        }
    )

    trace_provider = TracerProvider(resource=resource)
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

    print(f"📊 UC OTEL: traces → {tables['spans']!r} via {trace_ep}")
    print(f"📊 UC OTEL: metrics → {tables['metrics']!r} via {metric_ep}")
    print("✅ OpenTelemetry initialized (Databricks UC OTLP; logs not wired in this demo).")
    return OtelTelemetry(
        otel_config=uc_otel_config,
        trace_provider=trace_provider,
        meter_provider=meter_provider,
        tracer=tracer,
        meter=meter,
        trace_operation=trace_operation,
        record_metrics=record_metrics,
    )


def flush_databricks_uc_otel(ctx: ZerobusOtelContext) -> None:
    """Flush OTLP providers and persist Zerobus secret JSON if changed."""
    uc = ctx.telemetry.otel_config
    uc_tables = uc_otel_table_names(
        str(uc.get("UC_OTEL_CATALOG") or ""),
        str(uc.get("UC_OTEL_SCHEMA") or ""),
        str(uc.get("UC_OTEL_PREFIX") or ""),
    )
    print("\n📊 Flushing OTLP to Databricks UC tables…")
    ctx.telemetry.trace_provider.force_flush()
    ctx.telemetry.meter_provider.force_flush()
    ctx.save_config_if_changed()
    print("\n✅ UC OTEL export flush complete.")
    print("Query spans:", uc_tables["spans"])
    print("Query metrics:", uc_tables["metrics"])
    print("(Logs:", uc_tables["logs"], "— optional; wire OTLPLogExporter per docs/zerobus-ingest-otel-uc.md.)")
