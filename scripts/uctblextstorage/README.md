# `uctblextstorage` — UC managed location on customer cloud storage

Bootstrap Unity Catalog **storage credential**, **external location**, **grants**, and **catalog managed location** for Zerobus-style UC-managed Delta (see `docs/plan-external-managed-location-uc.md`).

## Layout

| File | Role |
|------|------|
| `env.example` | Copy to `.env` (or export vars); no secrets committed. |
| `external_managed_location_uc.sh` | **`source … <command>`** for all real commands (`discover`, **`bucket`**, **`role`**); **`./… help`** only for usage text (`.cursor/skills/databricks-cli-api/SKILL.md`). |
| `terraform/` | **AWS + UC** via Terraform; workspace auth uses **`~/.databrickscfg`** `profile` (see `terraform/README.md`). |

## Shell vs Terraform (bucket)

**Terraform** under `terraform/` already provisions the **S3 bucket** (with IAM and UC objects) in one workflow. Use **`source ./external_managed_location_uc.sh bucket`** only when you want an **incremental** shell-only step (e.g. walking the plan doc) or to align a bucket name with `terraform.tfvars` before apply—**not** as a second source of truth alongside an applied Terraform root for the same bucket without coordination.

## Quick start (shell discover)

Requires the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/), **[jq](https://jqlang.org/)**, and a workspace profile in `~/.databrickscfg` (host + token) for **`discover`**. For **`bucket`** / **`role`** on an **AWS** metastore, **[AWS CLI v2](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)** must be on `PATH`. Optional: set `DATABRICKS_PROFILE` to choose a non-default section.

```bash
cd scripts/uctblextstorage
cp env.example .env
# optional: uncomment DATABRICKS_PROFILE=… in .env

set -a && source .env && set +a
source ./external_managed_location_uc.sh discover
```

Run **`source ./external_managed_location_uc.sh <command>`** for **`discover`**, **`bucket`**, and **`role`** so **`UCTB_CLOUD`**, **`AWS_REGION`** (on AWS), **`UC_STORAGE_ROLE_ARN`** (after **`role`** on AWS), and **`main()`** strict mode stay in **your current shell**. Direct **`./… <command>`** is rejected except **`./… help`**.

**`bucket`** and **`role`** branch on **`UCTB_CLOUD`** from **`discover`** (aws implements S3 + IAM; azure/gcp print a not-implemented message—no **`aws`** required on the PATH for those clouds).

If **`BUCKET_NAME`** is not set after **`.env`**, **`discover`** sets it from **`databricks current-user me`** (**`userName`** before **`@`**, lowercased, **`.` → `-`**, non‑S3 characters folded to **`-`**, then **`-zerobus`** — hyphens because **S3 bucket names cannot contain `_`**).

**`UC_MASTER_ACCOUNT_ID` / `UC_MASTER_ROLE_NAME`** are the Unity Catalog **Principal** in your IAM trust policy (Databricks’ cross-account role, not your AWS account id). For **Databricks on AWS**, the manual states this **Principal is static** and shows the exact account and role per partition (e.g. commercial **`414351767826`** / **`unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL`**); GovCloud variants are on the same page. **`discover`** exports those **documented** values ([S3 external location — manual](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/s3/s3-external-location-manual)) and does not substitute a workspace API field for the trust policy. Override in **`.env`** if your partition differs (e.g. GovCloud DoD).

```bash
export BUCKET_NAME=my-org-zerobus-uc-unique
# Optional: put vars in scripts/uctblextstorage/.env — the script sources that file automatically when present.
set -a && source .env && set +a
source ./external_managed_location_uc.sh discover
source ./external_managed_location_uc.sh bucket
# After bucket exists: IAM_ROLE_NAME in .env; UC_MASTER_* from discover (doc defaults) unless set in .env
source ./external_managed_location_uc.sh role
```

If you use **Terraform** for the bucket and IAM, skip the matching shell commands.

## Quick start (Terraform, AWS)

Uses a **Databricks CLI profile** in `~/.databrickscfg` (not `DATABRICKS_*` env vars). See [terraform/README.md](terraform/README.md).

## Related doc

- [Plan: external managed location + UC](../../docs/plan-external-managed-location-uc.md)
