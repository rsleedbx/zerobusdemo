# Plan: `external_managed_location_uc` automation (Unity Catalog + S3)

Goal: reproducible setup so a **Unity Catalog catalog** can use a **managed location** on **customer S3** (storage credential + external location + grants), aligned with Zerobus UC-managed Delta requirements.

Reference flow (manual CLI today):

1. **Region / cloud** — `GET /api/2.1/unity-catalog/metastore_summary` on the target workspace (e.g. `databricks api get …/metastore_summary -p <profile> -o json`; host and token come from `~/.databrickscfg`).
2. **S3 bucket** — `aws s3api create-bucket` (region-specific flags; `us-east-1` omits `LocationConstraint`).
3. **IAM role** — trust policy allowing **Databricks UC metastore service** principal (`arn:aws:iam::<UC_MASTER_ACCOUNT_ID>:role/<UC_MASTER_ROLE_NAME>`) to `sts:AssumeRole`; inline S3 policy on that role for the bucket prefix. Shell (AWS metastore): **`source …/external_managed_location_uc.sh role`** after **`discover`** (exports `UC_STORAGE_ROLE_ARN`; optional `UC_TRUST_EXTERNAL_ID` matches Terraform `uc_trust_external_id`).
4. **Storage credential** — SQL: `CREATE STORAGE CREDENTIAL … AWS_IAM_ROLE` with `ROLE_ARN` = customer role.
5. **External location** — SQL: `CREATE EXTERNAL LOCATION … URL 's3://…/zerobus_ingest' WITH (CREDENTIAL …)`.
6. **Grants** — e.g. `GRANT CREATE MANAGED STORAGE ON EXTERNAL LOCATION … TO …`.
7. **Catalog** — SQL: `CREATE CATALOG … MANAGED LOCATION 's3://…/zerobus_ingest'`.

---

## Shell (`.sh`) vs Terraform

| Criterion | `.sh` + AWS CLI + `curl` + SQL | Terraform (+ optional small `.sh` wrapper) |
|-----------|-------------------------------|-----------------------------------------------|
| **Lines of code** | Lower for a one-off; grows with branching (regions, bucket exists, idempotency). | Higher upfront; stays flatter when resources multiply. |
| **Idempotency** | You implement it (`aws iam get-role`, ignore `EntityAlreadyExists`, SQL `IF NOT EXISTS` where supported). | Built-in for AWS IAM/S3; UC resources via `databricks_*` provider with drift detection. |
| **Secrets / tokens** | Env vars; easy to leak in logs if not careful. | Same env vars; state file must exclude secrets and live in secure backend. |
| **UC SQL objects** | Natural fit: `databricks sql execute` or API from shell. | `databricks_storage_credential`, `databricks_external_location`, `databricks_catalog` resources (provider version must match workspace capabilities). |
| **Trust policy constants** | UC master Principal is **Databricks-published static ARN per partition** (S3 external-location manual); shell sets from that doc (Gov hint) or `.env` override. | Same values in `terraform.tfvars` / variables; review when Databricks updates the manual. |
| **Maintenance** | One file is easy until AWS or Databricks changes flags or SQL syntax; then you grep scripts. | Provider changelog + `terraform plan` in CI catches more regressions. |

**Recommendation**

- **Shortest path to a working script once:** **`.sh`** calling `curl`, `aws`, and one SQL runner (Databricks SQL API or CLI `databricks sql`), with strict `set -euo pipefail` and explicit variables (`BUCKET_NAME`, `REGION`, `ROLE_NAME`, `WORKSPACE_HOST`, `CATALOG_NAME`, `EXTERNAL_LOCATION_NAME`, `STORAGE_CREDENTIAL_NAME`, trust ARNs from env).
- **Shortest path over months / multiple workspaces / CI:** **Terraform** for **S3 + IAM** (and optionally bucket policy if not inline-only), plus **`databricks_*`** resources for **storage credential, external location, catalog**; keep a **thin `.sh`** only for bootstrap (`terraform init`, workspace auth export, `terraform apply -var-file=…`). UC SQL can stay in Terraform if provider supports all properties you need; otherwise Terraform for infra + **minimal SQL** in `.sh` or Databricks job.

**Hybrid (often easiest to maintain):** Terraform for **S3 + IAM role + policies**; **shell or SQL file** for **Databricks UC** objects if your provider version lags catalog options or you prefer `CREATE …` from docs verbatim.

### Multi-cloud (AWS, Azure, GCP): how the recommendation changes

| Approach | AWS-only | + Azure + GCP |
|----------|----------|----------------|
| **Single `.sh`** | Reasonable for one cloud and one path. | **Weaker:** three CLIs (`aws`, `az`, `gcloud`), three bucket APIs, three trust models (IAM role trust to UC, Azure managed identity / federation to UC, GCP SA + IAM for GCS + UC). Branching and idempotency checks multiply; tests need three environments. |
| **Terraform** | Good when lifecycle and `plan` matter. | **Stronger:** one pattern — **per-cloud root module** (or `modules/aws`, `modules/azure`, `modules/gcp`) with the same **variable contract** (`bucket_name`, `prefix`, UC names, grantee). Cloud resources use `hashicorp/aws`, `hashicorp/azurerm`, `hashicorp/google`; UC side stays **`databricks_*`** with cloud-specific `storage_credential` / URL scheme (`s3://`, `abfss://`, `gs://`). |
| **Hybrid** | Terraform for AWS storage + IAM; shell for UC SQL if needed. | **Still good:** Terraform owns **each cloud’s storage + identity**; shared **SQL or `databricks_*`** for UC objects; avoid one mega-script calling three CLIs. |

**Concrete shift:** For tri-cloud, default recommendation moves from “**shell-first is fine**” to “**Terraform for cloud footprint + Databricks UC resources**, optional thin `.sh` for auth/bootstrap,” unless the team explicitly accepts owning **three parallel shell implementations** and their tests.

**UC side:** `CREATE STORAGE CREDENTIAL` / external location URL / credential type differ by cloud; keep **one doc or variables file per cloud** even inside Terraform (`tfvars.aws`, `tfvars.azure`, …).

**Metastore discovery:** `metastore_summary` stays workspace-scoped; use it to **validate** cloud/region against chosen module (wrong cloud → fail fast).

---

## Script phases (map to modules or functions)

1. **Inputs** — Databricks CLI profile (or default `~/.databrickscfg` section), AWS account id, `BUCKET_NAME`, path suffix (`zerobus_ingest`), UC trust identifiers (`UC_MASTER_ACCOUNT_ID`, `UC_MASTER_ROLE_NAME`), principal to grant (parameterize), catalog name (`zerobus_ingest`).
2. **Discover** — `metastore_summary` (CLI or API) → persist `region` / cloud for validation and bucket `LocationConstraint` choice.
3. **AWS** — Bucket (create if missing), IAM role + trust + inline policy (idempotent checks).
4. **Databricks UC** — Storage credential → external location → grants → catalog `MANAGED LOCATION` (order matters; catalog requires location + permissions).
5. **Verify** — Optional: `DESCRIBE EXTERNAL LOCATION`, `SHOW GRANTS`, or small `SELECT` in new catalog.

---

## Risks and decisions (capture before implementation)

- **Trust policy** must match the **exact** UC metastore IAM principal for that deployment (copy from **Databricks Unity Catalog + S3 external storage** docs for your **cloud/partition** — e.g. commercial AWS `arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL` for typical `*.cloud.databricks.com` workspaces; GovCloud uses separate published ARNs). There is no workspace-only API that replaces the docs; wrong ARN fails `AssumeRole`.
- **S3 bucket ownership / public access** — default private; block public access; optional KMS (not in your sketch; add if org requires CMK).
- **SQL execution context** — SQL warehouse or serverless SQL; PAT vs OAuth for non-interactive runs.
- **Naming** — Single place for `zerobus_loc` / `zerobus_s3_cred` / `zerobus_ingest` to avoid drift with shell heredocs.
- **Non-AWS** — This plan is AWS-only; Azure/GCP need different storage + credential types; split doc or per-cloud directory when extending.

---

## Deliverables (suggested)

| Deliverable | Purpose |
|-------------|---------|
| `scripts/uctblextstorage/env.example` | Documented variables (no secrets) for shell discover. |
| `scripts/uctblextstorage/terraform/` | **AWS + UC** Terraform; workspace auth via `~/.databrickscfg` profile. |
| `scripts/uctblextstorage/external_managed_location_uc.sh` | Shell: **`source … <command>`** for **discover**, **bucket**, **role** (cloud branch from **`UCTB_CLOUD`**; exports + caller shell options); **`./… help`** only. Databricks via CLI profile. |
| `docs/plan-external-managed-location-uc.md` (this file) | Decision record; link from main README or `docs/faq` if it becomes operational runbook. |

---

## Next step

Pick **shell-only v1** vs **Terraform + thin shell** for the first merge; implement **AWS + single workspace** under `scripts/uctblextstorage/`; add **dry-run** mode that prints `aws`/`curl`/SQL without executing until trust ARNs and bucket names are confirmed.
