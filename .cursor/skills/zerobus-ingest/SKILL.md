---
name: zerobus-ingest
description: zerobusdemo repo specifics for ZeroBus — table names, notebook paths, zbhelper file layout, and benchmark driver. Use when editing zerobusdemo notebooks or the zbhelper library, or when the user asks which table, notebook, or source file to look at for a given ingest pattern.
---

# ZeroBus — zerobusdemo repo

For ZeroBus patterns (auth, HTTP, gRPC, benchmark structure), see the global `databricks-zerobus` skill.

## Tables

| Notebook pattern | Table |
|---|---|
| gRPC sync | `main.robert_lee.airquality_grpc_sync` |
| gRPC async | `main.robert_lee.airquality_grpc_async` |
| HTTP sync | `main.robert_lee.airquality_http_sync` |
| HTTP async | `main.robert_lee.airquality_http_async` |

`TABLE_NAME = f"{CATALOG}.{SCHEMA}.{TABLE}"` — configured in the setup cells of each notebook.

## Notebooks

| File | Pattern |
|---|---|
| `notebooks/zerobus_grpc_sync.ipynb` | gRPC, synchronous SDK |
| `notebooks/zerobus_grpc_async.ipynb` | gRPC, async SDK |
| `notebooks/zerobus_http_sync.ipynb` | HTTP REST, requests.Session |
| `notebooks/zerobus_http_async.ipynb` | HTTP REST, aiohttp + asyncio.gather |
| `notebooks/zerobus_benchmark_driver.ipynb` | Driver: runs all 4 patterns in a loop |

All four demo notebooks share the same 4a/4b/4c/4d cell structure (see global `benchmark.md`).

## zbhelper library (`src/zbhelper/`)

| File | Role |
|---|---|
| `zerobus_ingest.py` | `IngestConfig`, `ingest_dataframe`, `build_zerobus_endpoint` |
| `protobuf_converter.py` | Schema → `.proto` → compiled message class → bytes |
| `ingest_benchmark.py` | Reusable functions for all 4 benchmark patterns |
| `__init__.py` | Re-exports public API |

## OTel notebooks

| File | Purpose |
|---|---|
| `notebooks/otel/grafana/secrets-bootstrap.ipynb` | Provision Databricks secrets for OTel credentials |
| `notebooks/otel/grafana/zerobus-otel.ipynb` | ZeroBus + OTel end-to-end with Grafana |
| `notebooks/otel/uc/zerobus-uc-otel.ipynb` | Unity Catalog variant |
| `notebooks/otel/jaeger/jaeger.sh` | Local Jaeger for trace visualization |
