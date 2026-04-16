---
name: zerobus-ingest
description: ZeroBus ingest patterns specific to the zerobusdemo repo — notebook cell structure (4a singles / 4b batch / 4c visibility / 4d metrics), benchmark configuration, DemoAckCallback, zbhelper module layout, and Unity Catalog table naming conventions used here. Use when editing or adding to the zerobusdemo notebooks or zbhelper library, or when the user asks about the demo structure, timing metrics, or batch run configuration.
---

# ZeroBus Ingest — zerobusdemo specifics

For general ZeroBus auth, endpoint, HTTP, and gRPC patterns, see the global `databricks-zerobus` skill.

---

## Table naming (this repo)

```python
TABLE_NAME = f"{CATALOG}.{SCHEMA}.{TABLE}"  # e.g. main.robert_lee.airquality_grpc_sync
```

Each notebook uses a distinct table name suffix: `airquality_grpc_sync`, `airquality_grpc_async`, `airquality_http_sync`, `airquality_http_async`.

---

## Demo notebook structure (4 steps)

All four notebooks (`grpc_sync`, `grpc_async`, `http_sync`, `http_async`) share the same cell layout:

| Cell | Name | Purpose |
|---|---|---|
| 18 | Setup | Config, session/stream open, `_n`, `_singles`, `_batch_runs` |
| 20 | 4a singles | `_singles` individual inserts; captures `_row_send_seconds`, `_row_wait_seconds` |
| 22 | 4b batch | `_batch_runs` consecutive batch sends; captures `_batch_run_seconds` list |
| 24 | 4c visibility | Poll `COUNT(*)` until `_target_count`; compute `_visibility_s` and `_visibility_from_first_send_s` |
| 26 | 4d key metrics | Print all timings with min/mean/median/max distributions |

### Key config variables (cell 18)

```python
_n = 1000           # total rows per run
_singles = min(10, _n)  # rows sent individually in 4a
_batch_runs = 10    # consecutive batch sends in 4b (configurable)
```

### Timing variables (cells 20–24)

```python
t_4a0               # perf_counter() at first single row send
_singles_wall_s     # total wall time for all singles
_row_send_seconds   # list[float]: per-row send duration
_row_wait_seconds   # list[float]: per-row wait/ack duration (0.0 for HTTP)
_batch_run_seconds  # list[float]: wall time per batch run
_batch_send_seconds # list[float]: send portion per batch run
_batch_wait_seconds # list[float]: wait portion per batch run (0.0 for HTTP)
_total_rows_inserted = _singles + _batch_n * len(_batch_run_seconds)
_ingest_4a4b_s = _singles_wall_s + sum(_batch_run_seconds)
_t_after_close      # perf_counter() after stream.close() / last insert
_visibility_s       # time from _t_after_close → COUNT(*) reached target
_visibility_from_first_send_s  # time from t_4a0 → COUNT(*) reached target
```

### 4b distribution print (cell 26 — identical across all 4 notebooks)

```python
if _batch_run_seconds and _batch_n:
    print(
        f"  4b ({_batch_n} batched, {len(_batch_run_seconds)} runs):"
        f"  wall  min={min(_batch_run_seconds)*1000:.1f} ms"
        f"  mean={mean(_batch_run_seconds)*1000:.1f} ms"
        f"  median={median(_batch_run_seconds)*1000:.1f} ms"
        f"  max={max(_batch_run_seconds)*1000:.1f} ms"
    )
```

---

## DemoAckCallback (gRPC notebooks)

```python
class DemoAckCallback(AckCallback):
    def on_ack(self, offset: int) -> None:
        print(f"  [ack] offset={offset}")
    def on_error(self, error: Exception) -> None:
        print(f"  [ack-error] {error}")
```

Passed to `sdk.create_stream(ack_callback=DemoAckCallback())`. Used in both `grpc_sync` and `grpc_async`.

---

## zbhelper module layout (`src/zbhelper/`)

| File | Role |
|---|---|
| `zerobus_ingest.py` | `IngestConfig`, `ingest_dataframe`, `build_zerobus_endpoint` |
| `protobuf_converter.py` | Schema → `.proto` → compiled message class → bytes |
| `ingest_benchmark.py` | Reusable benchmark functions for all 4 patterns |
| `__init__.py` | Re-exports public API |

Driver notebook: `notebooks/zerobus_benchmark_driver.ipynb`

---

## HTTP session reuse (this repo's pattern)

gRPC and HTTP sessions are opened in cell 18 and closed at the end of cell 22. The token fetch for HTTP reuses the same `requests.Session` as the inserts — the session is warm by the time the first insert runs.

---

## Visibility polling (cell 24 — identical across notebooks)

```python
_target_count = _row_before + _total_rows_inserted
_poll_deadline = _t_after_close + 120.0
while time.perf_counter() < _poll_deadline:
    _row_visible = spark.sql(f"SELECT COUNT(*) AS c FROM {TABLE_NAME}").collect()[0]["c"]
    if _row_visible >= _target_count:
        break
    time.sleep(0.15)
```
