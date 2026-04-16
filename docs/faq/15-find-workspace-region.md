# How do I find my workspace region to construct the ZeroBus endpoint?

```
https://<workspace_id>.zerobus.<region>.<cloud-domain>
         ────────────           ──────
              │                    │
              └─ (1) workspace_id  └─ (2) region
```

`SELECT current_metastore()` requires Unity Catalog for that Spark session. If it raises (for example `OPERATION_REQUIRES_UNITY_CATALOG`), set **`DATABRICKS_REGION_OVERRIDE`** at the top of Step 2.a in `notebooks/zerobus_grpc_async.ipynb`, `notebooks/zerobus_grpc_sync.ipynb`, or `notebooks/zerobus_http_sync.ipynb` to the workspace region (`eastus2`, `us-west-2`). Values that look like availability zones (`us-west-2a`) are normalized to a region label in Step 2.a.

`clusters.list_zones()` is not used on Azure (Databricks returns `Azure worker environment does not support zones`).

Select your cloud: [AWS](#aws) · [Azure](#azure) · [GCP](#gcp)

> Replace `DEFAULT` with your profile name if using a named profile in `~/.databrickscfg`.

---

## AWS

**Endpoint format:**

```
https://<workspace_id>.zerobus.<region>.cloud.databricks.com
```

**In-notebook (same region source as Step 2.a in the gRPC/HTTP demo notebooks when `DATABRICKS_REGION_OVERRIDE` is blank):**

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient(profile="DEFAULT")
workspace_id = str(w.get_workspace_id())
workspace_url = w.config.host.rstrip("/")
region = spark.sql("SELECT current_metastore()").collect()[0][0].split(":")[1]
zerobus_endpoint = f"https://{workspace_id}.zerobus.{region}.cloud.databricks.com"
print(f"ZEROBUS_SERVER_ENDPOINT={zerobus_endpoint}")
print(f"DATABRICKS_WORKSPACE_URL={workspace_url}")
```

The metastore string is `cloud:<region>:…`; the region is the second colon-delimited segment.

**CLI:**

```bash
WORKSPACE_URL=$(databricks auth env --profile DEFAULT | jq -r '.env.DATABRICKS_HOST')
REGION=$(databricks api get /api/2.0/unity-catalog/metastore_summary --profile DEFAULT | jq -r '.region')
WORKSPACE_ID=$(python3 -c 'from databricks.sdk import WorkspaceClient; print(WorkspaceClient(profile="DEFAULT").get_workspace_id())')
echo "ZEROBUS_SERVER_ENDPOINT=https://${WORKSPACE_ID}.zerobus.${REGION}.cloud.databricks.com"
echo "DATABRICKS_WORKSPACE_URL=${WORKSPACE_URL}"
```

---

## Azure

**Endpoint format:**

```
https://<workspace_id>.zerobus.<region>.azuredatabricks.net
```

**In-notebook:**

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient(profile="DEFAULT")
workspace_id = str(w.get_workspace_id())
workspace_url = w.config.host.rstrip("/")
region = spark.sql("SELECT current_metastore()").collect()[0][0].split(":")[1]
zerobus_endpoint = f"https://{workspace_id}.zerobus.{region}.azuredatabricks.net"
print(f"ZEROBUS_SERVER_ENDPOINT={zerobus_endpoint}")
print(f"DATABRICKS_WORKSPACE_URL={workspace_url}")
```

**Workspace ID from hostname:**

Digits between `adb-` and the first `.` in the hostname match the workspace id segment Databricks uses in URLs:

```
Hostname: adb-1234567890123456.12.azuredatabricks.net
               ────────────────
               workspace_id = 1234567890123456
```

**CLI:**

```bash
WORKSPACE_URL=$(databricks auth env --profile DEFAULT | jq -r '.env.DATABRICKS_HOST')
WORKSPACE_ID=$(python3 -c 'from databricks.sdk import WorkspaceClient; print(WorkspaceClient(profile="DEFAULT").get_workspace_id())')
REGION=$(databricks api get /api/2.0/unity-catalog/metastore_summary --profile DEFAULT | jq -r '.region')
echo "ZEROBUS_SERVER_ENDPOINT=https://${WORKSPACE_ID}.zerobus.${REGION}.azuredatabricks.net"
echo "DATABRICKS_WORKSPACE_URL=${WORKSPACE_URL}"
```

---

## GCP

**Endpoint format:**

```
https://<workspace_id>.zerobus.<region>.gcp.databricks.com
```

**Workspace hostname:** `https://<workspace_id>.<shard>.gcp.databricks.com` — the workspace id is the first numeric segment before the first `.`.

**In-notebook:** Step 2.a in `notebooks/zerobus_grpc_async.ipynb`, `notebooks/zerobus_grpc_sync.ipynb`, and `notebooks/zerobus_http_sync.ipynb` takes the workspace id from that hostname pattern when the host matches `*.gcp.databricks.com`; otherwise it uses `get_workspace_id()`. Region still comes from `DATABRICKS_REGION_OVERRIDE` or `current_metastore()` like the other clouds.

**CLI:**

```bash
WORKSPACE_URL=$(databricks auth env --profile DEFAULT | jq -r '.env.DATABRICKS_HOST')
WORKSPACE_ID=$(python3 -c 'from databricks.sdk import WorkspaceClient; print(WorkspaceClient(profile="DEFAULT").get_workspace_id())')
REGION=$(databricks api get /api/2.0/unity-catalog/metastore_summary --profile DEFAULT | jq -r '.region')
echo "ZEROBUS_SERVER_ENDPOINT=https://${WORKSPACE_ID}.zerobus.${REGION}.gcp.databricks.com"
echo "DATABRICKS_WORKSPACE_URL=${WORKSPACE_URL}"
```

---

## Demo notebooks (Step 2.a)

`notebooks/zerobus_grpc_async.ipynb`, `notebooks/zerobus_grpc_sync.ipynb`, and `notebooks/zerobus_http_sync.ipynb` share one Step 2.a block: `DATABRICKS_WORKSPACE_URL`, `DATABRICKS_WORKSPACE_ID`, `DATABRICKS_WORKSPACE_O_QUERY` (`?o=<workspace_id>`), `DATABRICKS_WORKSPACE_SP_UI_PREFIX` (workspace settings path for service principals), `_domain_for_host`, then `DATABRICKS_REGION` from `DATABRICKS_REGION_OVERRIDE` or `SELECT current_metastore()`, then `ZEROBUS_INGEST_URL` / `SERVER_ENDPOINT`. A failed `current_metastore()` prints a skip line; if the region is still empty, Step 2.a raises.

After Step 2.b / 2.c / DDL, those notebooks `print` workspace URLs (service principal, SP secrets, Data Explorer table) using the same `DATABRICKS_WORKSPACE_URL` and `DATABRICKS_WORKSPACE_O_QUERY`.

---

*Source: [Get your workspace URL and ZeroBus Ingest endpoint](https://docs.databricks.com/aws/en/ingestion/zerobus-ingest#get-your-workspace-url-and-zerobus-ingest-endpoint)*

*[← Back to FAQ index](../zerobus_faq.md)*
