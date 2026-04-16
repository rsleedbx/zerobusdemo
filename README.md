# zerobusdemo

Demo and helper code for **Databricks ZeroBus Ingest** — stream Protobuf-serialized rows
directly into Unity Catalog Delta tables without Kafka, brokers, or partition management.

## What's in this repo

| Path | Purpose |
|------|---------|
| `src/zbhelper/` | Python helpers: ZeroBus ingest, Protobuf converter, workspace info |
| `src/statschema/` | DDL/stats transpiler (bundled; will move to PyPI as `statschema`) |
| `notebooks/zerobus_grpc_async.ipynb`, `zerobus_grpc_sync.ipynb`, `zerobus_http_sync.ipynb` | Zerobus doc-style demos (gRPC async/sync, HTTP REST) |
| `docs/faq/` | ZeroBus FAQ (authentication, serialization, throughput, limits, …) |
| `zerobus-sdk/python/` | ZeroBus Python SDK source |
| `databricks.yml` | Databricks asset bundle config |

## Quick start

```bash
# 1. Create a virtual environment
make venv

# 2. Configure Databricks credentials
databricks auth login --configure-serverless

# 3. Run unit tests (no credentials needed)
make test-unit

# 4. Run integration test against a live workspace
export ZEROBUS_TABLE_NAME=main.my_schema.my_table
make test-integration
```

## Core API

```python
from src.zbhelper import IngestConfig, ingest_dataframe
from src.statschema import parse_ddl

# Parse schema from any dialect
tables = parse_ddl("CREATE TABLE orders (id INT, amount DECIMAL(10,2))", dialect="mysql")

# Build config (uses ~/.databrickscfg automatically)
config = IngestConfig.from_workspace_client(table_name="main.default.orders")

# Stream a Spark DataFrame into ZeroBus
result = ingest_dataframe(df, tables[0], config)
print(f"Sent {result.rows_sent} rows in {result.batch_count} batches")
```

## Authentication

Two modes are supported:

**SDK auth (local dev)** — uses `~/.databrickscfg`, same credentials as Databricks Connect:
```python
config = IngestConfig.from_workspace_client(table_name="main.default.t")
```

**Service principal (CI/production)** — requires env vars:
```bash
export DATABRICKS_CLIENT_ID=...
export DATABRICKS_CLIENT_SECRET=...
export ZEROBUS_SERVER_ENDPOINT=https://...
export DATABRICKS_WORKSPACE_URL=https://...
export ZEROBUS_TABLE_NAME=main.schema.table
```
```python
config = IngestConfig.from_env()
```

## Sequence Diagram
```mermaid
sequenceDiagram
    autonumber
    participant App      as Client app
    participant SDK      as Zerobus SDK (client)
    participant Service  as Zerobus Ingest service
    participant WAL      as WAL / durable buffer
    participant Rej      as _zerobus/table_rejected_parquets
    participant Delta    as Delta table

    Note over App,SDK: ingest_record_offset (client-side enqueue only)

    App->>SDK: ingest_record_offset(record_dict)
    SDK->>SDK: Serialize record_dict\nGenerate local offset (OffsetIdGenerator.next)
    SDK->>SDK: Enqueue (offset, payload)\ninto SDK internal buffer/queue
    SDK-->>App: return offset\n(SDK has accepted & queued it)

    Note over SDK,Service: network send happens after return (async)

    SDK->>Service: gRPC send(offset, payload)\n(async / background)

    Note over Service,WAL: server-side durability in WAL

    Service->>WAL: append(offset, record)\n(make record durable in WAL/buffer)

    Note over App,Service: Durability is known only after server ack,\nthen via ack callback or wait/flush

    %% Single server ack event
    Service-->>SDK: durability ack up to offset N

    %% 1st client-visible effect: async AckCallback (if configured)
    SDK-->>App: on_ack(offset) callback

    %% 2nd client-visible effect: explicit wait/flush completes
    App->>SDK: wait_for_offset(offset) / flush / close
    SDK-->>App: wait_for_offset / flush / close returns\n(last_ack >= offset)

    Note over WAL,Delta: Later, background materialization based on durable WAL

    alt materialization to main table succeeds
        WAL->>Delta: write optimized Delta files\n(background, post-ack)
    else schema/table issue after durability
        WAL->>Rej: write Parquet\nunder _zerobus/table_rejected_parquets
    end
```    

## FAQ

See [`docs/faq/`](docs/faq/) for answers to common questions:

- [What is ZeroBus Ingest?](docs/faq/01-what-is-zerobus.md)
- [Authentication modes](docs/faq/05-authentication.md)
- [Serialization formats](docs/faq/06-serialization-formats.md)
- [Throughput and latency](docs/faq/07-throughput-latency.md)
- [Key limitations](docs/faq/12-key-limitations.md)
- [Find your workspace region](docs/faq/15-find-workspace-region.md)

## Dependency note

`src/statschema/` is bundled here for convenience until the
[statschema](https://github.com/robert-lee/statschema) package is published to PyPI.
Once available, replace it with `pip install statschema` and update imports accordingly.
