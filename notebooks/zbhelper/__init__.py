"""
zbhelper — Databricks / ZeroBus helper utilities.

Submodules (import directly — not eagerly loaded here to avoid
pulling in heavy or unavailable dependencies like statschema):

    zbhelper.ingest_benchmark     JSON record benchmark patterns
    zbhelper.zerobus_ingest       serialize a Spark DataFrame and stream into ZeroBus
    zbhelper.protobuf_converter   canonical schema → .proto + Spark Row → protobuf bytes
    zbhelper.workspace_region_info  retrieve workspace region and ZeroBus endpoints
"""
