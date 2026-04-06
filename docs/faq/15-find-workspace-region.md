# How do I find my workspace region to construct the ZeroBus endpoint?

```
https://<workspace_id>.zerobus.<region>.<cloud-domain>
         ────────────           ──────
              │                    │
              └─ (1) workspace_id  └─ (2) region
```

Select your cloud: [AWS](#aws) · [Azure](#azure)

> Replace `DEFAULT` with your profile name if using a named profile in `~/.databrickscfg`.

---

## AWS

**Endpoint format:**
```
https://<workspace_id>.zerobus.<region>.cloud.databricks.com
```

**In-notebook (recommended):**
```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
workspace_id  = w.get_workspace_id()
workspace_url = w.config.host
region        = w.clusters.list_zones().default_zone[:-1]  # strip trailing AZ letter
zerobus_endpoint = f"https://{workspace_id}.zerobus.{region}.cloud.databricks.com"
print(f"ZEROBUS_SERVER_ENDPOINT={zerobus_endpoint}")
print(f"DATABRICKS_WORKSPACE_URL={workspace_url}")
```

`default_zone` is an availability zone (`us-west-2a`); the region is the value without the trailing letter (`us-west-2`).

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

> **Note:** `clusters.list_zones()` raises `BadRequest: Azure worker environment does not support zones` on Azure — do not use it for Azure region lookup.

**In-notebook (recommended):**

The Unity Catalog metastore name encodes the region in its second colon-delimited segment (e.g. `cloud:eastus2:uuid`):

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
workspace_id     = w.get_workspace_id()
workspace_url    = w.config.host
region           = spark.sql("SELECT current_metastore()").collect()[0][0].split(":")[1]
zerobus_endpoint = f"https://{workspace_id}.zerobus.{region}.azuredatabricks.net"
print(f"ZEROBUS_SERVER_ENDPOINT={zerobus_endpoint}")
print(f"DATABRICKS_WORKSPACE_URL={workspace_url}")
```

**Workspace ID from hostname:**

The workspace ID is the digits between `adb-` and the first `.` in the hostname:
```
Hostname: adb-1234567890123456.12.azuredatabricks.net
               ────────────────
               workspace_id = 1234567890123456
```

**CLI (region from metastore):**
```bash
WORKSPACE_URL=$(databricks auth env --profile DEFAULT | jq -r '.env.DATABRICKS_HOST')
WORKSPACE_ID=$(python3 -c 'from databricks.sdk import WorkspaceClient; print(WorkspaceClient(profile="DEFAULT").get_workspace_id())')
REGION=$(databricks api get /api/2.0/unity-catalog/metastore_summary --profile DEFAULT | jq -r '.region')
echo "ZEROBUS_SERVER_ENDPOINT=https://${WORKSPACE_ID}.zerobus.${REGION}.azuredatabricks.net"
echo "DATABRICKS_WORKSPACE_URL=${WORKSPACE_URL}"
```

---

## Notebook auto-derivation

`new_public_example.ipynb` derives both values automatically. The relevant logic:

```python
workspace_id = w.get_workspace_id()

if "azuredatabricks.net" in workspace_url:
    # Azure: region from Unity Catalog metastore name
    region = spark.sql("SELECT current_metastore()").collect()[0][0].split(":")[1]
    domain = "azuredatabricks.net"
else:
    # AWS: region from cluster zones (strip trailing AZ letter)
    region = w.clusters.list_zones().default_zone[:-1]
    domain = "cloud.databricks.com"

zerobus_endpoint = f"https://{workspace_id}.zerobus.{region}.{domain}"
```

If `ZEROBUS_SERVER_ENDPOINT` is already set in `config.json`, auto-derivation is skipped.

---

*Source: [Get your workspace URL and ZeroBus Ingest endpoint](https://docs.databricks.com/aws/en/ingestion/zerobus-ingest#get-your-workspace-url-and-zerobus-ingest-endpoint)*

*[← Back to FAQ index](../zerobus_faq.md)*
