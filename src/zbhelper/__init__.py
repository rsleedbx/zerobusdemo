"""
zbhelper — Databricks / ZeroBus helper utilities.

Submodules
----------
protobuf_converter   canonical schema → .proto + Spark Row → protobuf bytes
zerobus_ingest       serialize a Spark DataFrame and stream it into ZeroBus
workspace_region_info  retrieve workspace region, zones, and ZeroBus endpoints
ingest_benchmark     JSON record benchmark patterns matching the four demo notebooks
"""

from .protobuf_converter import (
    schema_to_proto_str,
    compile_proto,
    row_to_message,
    serialize_row,
)
from .zerobus_ingest import (
    IngestConfig,
    IngestResult,
    ingest_dataframe,
    build_qualified_table_name,
    build_zerobus_endpoint,
    DatabricksSdkHeadersProvider,
)
from .workspace_region_info import (
    WorkspaceInfoRetriever,
    get_workspace_info,
    save_workspace_info_to_file,
)
from .ingest_benchmark import (
    SinglesResult,
    BatchResult,
    VisibilityResult,
    json_payload_bytes,
    build_records,
    resolve_sp_config,
    fetch_row_baseline,
    DemoBenchmarkAckCallback,
    open_grpc_stream_sync,
    ingest_singles_grpc_sync,
    ingest_batch_and_close_grpc_sync,
    open_grpc_stream_async,
    ingest_singles_grpc_async,
    ingest_batch_and_close_grpc_async,
    fetch_http_token,
    http_insert_url,
    ingest_singles_http_sync,
    ingest_batch_http_sync,
    ingest_singles_http_async,
    ingest_batch_http_async,
    poll_visibility,
    print_metrics,
)

__all__ = [
    # protobuf_converter
    "schema_to_proto_str",
    "compile_proto",
    "row_to_message",
    "serialize_row",
    # zerobus_ingest
    "IngestConfig",
    "IngestResult",
    "ingest_dataframe",
    "build_qualified_table_name",
    "build_zerobus_endpoint",
    "DatabricksSdkHeadersProvider",
    # workspace_region_info
    "WorkspaceInfoRetriever",
    "get_workspace_info",
    "save_workspace_info_to_file",
    # ingest_benchmark
    "SinglesResult",
    "BatchResult",
    "VisibilityResult",
    "json_payload_bytes",
    "build_records",
    "resolve_sp_config",
    "fetch_row_baseline",
    "DemoBenchmarkAckCallback",
    "open_grpc_stream_sync",
    "ingest_singles_grpc_sync",
    "ingest_batch_and_close_grpc_sync",
    "open_grpc_stream_async",
    "ingest_singles_grpc_async",
    "ingest_batch_and_close_grpc_async",
    "fetch_http_token",
    "http_insert_url",
    "ingest_singles_http_sync",
    "ingest_batch_http_sync",
    "ingest_singles_http_async",
    "ingest_batch_http_async",
    "poll_visibility",
    "print_metrics",
]
