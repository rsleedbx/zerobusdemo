# ZeroBus Ingest OpenTelemetry → Unity Catalog (reference)

This path sends OTLP **http/protobuf** from a standard OpenTelemetry client to the **Databricks workspace** API. Telemetry lands in **managed Delta tables** in Unity Catalog. It is **not** the same as `notebooks/otel/grafana/zerobus-otel.ipynb`, which exports OTLP to **Grafana Cloud**.

Status: described as preview / Beta in internal product material; follow current Databricks docs and PrD/PrPr for DDL and policy.

## Demo notebook

`notebooks/otel/uc/zerobus-uc-otel.ipynb` and `notebooks/otel/uc/zerobus_uc_otel_lab.py`. Restart the kernel after running `notebooks/otel/grafana/zerobus-otel.ipynb` in the same session (global OpenTelemetry providers).

---

## 1. Unity Catalog target tables

Create three **managed Delta** tables in UC (spans, logs, metrics). Naming pattern:

- `<catalog>.<schema>.<prefix>_otel_spans`
- `<catalog>.<schema>.<prefix>_otel_logs`
- `<catalog>.<schema>.<prefix>_otel_metrics`

Each table:

```sql
CREATE TABLE <catalog>.<schema>.<prefix>_otel_spans ( ... ) USING DELTA
TBLPROPERTIES ('otel.schemaVersion' = 'v1');

CREATE TABLE <catalog>.<schema>.<prefix>_otel_logs ( ... ) USING DELTA
TBLPROPERTIES ('otel.schemaVersion' = 'v1');

CREATE TABLE <catalog>.<schema>.<prefix>_otel_metrics ( ... ) USING DELTA
TBLPROPERTIES ('otel.schemaVersion' = 'v1');
```

Full column DDLs are in the PrD / PrPr guide; do not invent schemas here.

---

## 2. Token and permissions

Use a **PAT** or **service principal** token for the workspace. Grant:

- `USE CATALOG` on `<catalog>`
- `USE SCHEMA` on `<catalog>.<schema>`
- `SELECT` and `MODIFY` on each of the three OTEL tables

---

## 3. OTLP HTTP endpoints

Host is the **workspace URL** (AWS-style example below; Azure uses `https://<workspace>.azuredatabricks.net` with the same path suffix).

| Signal   | URL |
|----------|-----|
| Traces   | `https://<workspace>.cloud.databricks.com/api/2.0/otel/v1/traces` |
| Logs     | `https://<workspace>.cloud.databricks.com/api/2.0/otel/v1/logs` |
| Metrics  | `https://<workspace>.cloud.databricks.com/api/2.0/otel/v1/metrics` |

Protocol: **http/protobuf**.

---

## 4. Exporter headers

On **each** exporter (traces, logs, metrics):

| Header | Value |
|--------|--------|
| `content-type` | `application/x-protobuf` |
| `X-Databricks-UC-Table-Name` | `<catalog>.<schema>.<prefix>_otel_spans` \| `..._otel_logs` \| `..._otel_metrics` (match signal) |
| `Authorization` | `Bearer <token>` |

Newer **App Telemetry Profile** flows may use `X-Databricks-OTEL-Config=<profile_name>` instead of per-request UC table headers; confirm against the current OTEL PRD and what is live in the PrPr guide.

---

## 5. Python: OTLP HTTP exporters

```python
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter

# Logs: package/version-dependent; often:
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter

WORKSPACE_URL = "https://<workspace>.cloud.databricks.com"
CATALOG = "<catalog>"
SCHEMA = "<schema>"
PREFIX = "my_prefix"
TOKEN = "<DATABRICKS_TOKEN>"

common_headers = {
    "content-type": "application/x-protobuf",
    "Authorization": f"Bearer {TOKEN}",
}

trace_exporter = OTLPSpanExporter(
    endpoint=f"{WORKSPACE_URL}/api/2.0/otel/v1/traces",
    headers={
        **common_headers,
        "X-Databricks-UC-Table-Name": f"{CATALOG}.{SCHEMA}.{PREFIX}_otel_spans",
    },
)

log_exporter = OTLPLogExporter(
    endpoint=f"{WORKSPACE_URL}/api/2.0/otel/v1/logs",
    headers={
        **common_headers,
        "X-Databricks-UC-Table-Name": f"{CATALOG}.{SCHEMA}.{PREFIX}_otel_logs",
    },
)

metric_exporter = OTLPMetricExporter(
    endpoint=f"{WORKSPACE_URL}/api/2.0/otel/v1/metrics",
    headers={
        **common_headers,
        "X-Databricks-UC-Table-Name": f"{CATALOG}.{SCHEMA}.{PREFIX}_otel_metrics",
    },
)
```

Wire these exporters into `TracerProvider`, log/metric pipelines, and `MeterProvider` per OpenTelemetry Python docs. Logs exporter import path depends on `opentelemetry-exporter-otlp` version.

---

## 6. Zero-code: `opentelemetry-instrument` (example)

```bash
export OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED=true

opentelemetry-instrument \
  --service_name <service_name> \
  --metrics_exporter otlp \
  --traces_exporter otlp \
  --logs_exporter otlp \
  --exporter_otlp_protocol http/protobuf \
  --exporter_otlp_logs_endpoint    "https://<workspace>.cloud.databricks.com/api/2.0/otel/v1/logs" \
  --exporter_otlp_traces_endpoint  "https://<workspace>.cloud.databricks.com/api/2.0/otel/v1/traces" \
  --exporter_otlp_metrics_endpoint "https://<workspace>.cloud.databricks.com/api/2.0/otel/v1/metrics" \
  --exporter_otlp_logs_headers    "content-type=application/x-protobuf,X-Databricks-UC-Table-Name=<catalog>.<schema>.<prefix>_otel_logs,Authorization=Bearer <token>" \
  --exporter_otlp_traces_headers  "content-type=application/x-protobuf,X-Databricks-UC-Table-Name=<catalog>.<schema>.<prefix>_otel_spans,Authorization=Bearer <token>" \
  --exporter_otlp_metrics_headers "content-type=application/x-protobuf,X-Databricks-UC-Table-Name=<catalog>.<schema>.<prefix>_otel_metrics,Authorization=Bearer <token>" \
  flask run -p 8080
```

Replace workspace host, catalog, schema, prefix, token, and app command as needed.
