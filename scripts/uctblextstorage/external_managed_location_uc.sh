#!/usr/bin/env bash
# UC table external storage — plan steps as commands (Unity Catalog + customer cloud).
# Usage: source … <command>  (only ./… help is supported without source)
#
# discover:  plan §1 — metastore_summary (JSON); exports UCTB_CLOUD (+ UCTB_REGION, AWS_REGION on aws; UC_MASTER_* from Databricks S3 external-location manual static Principal).
# bucket:    plan §2 — cloud-specific storage bucket step (aws: S3; azure/gcp: not implemented here).
# role:      plan §3 — cloud-specific access role (aws: IAM trust + inline S3 prefix; azure/gcp: not implemented here).
#   Prefer terraform/ for bucket + IAM + UC in one apply; shell is incremental / debugging (same names as tfvars).
#
# Auth: ~/.databrickscfg via optional DATABRICKS_PROFILE for Databricks commands only.

# Strict mode only when executed — sourcing must not change the interactive shell's options.
[[ "${BASH_SOURCE[0]}" == "${0}" ]] && set -euo pipefail

# Run argv as one command. Always "$@" (separate words); never "$*" (merges args and breaks CLIs).
run() { "$@"; }

# Log argv to stderr (shell-quoted words), then run — same "$@" rules as run.
run_log() {
  printf '%q ' "$@" >&2
  echo >&2
  "$@"
}

_uctb_s3_browse_url_line() {
  printf 'browse: https://s3.console.aws.amazon.com/s3/buckets/%s?region=%s\n' "${1}" "${2}" >&2
}

# Real commands must be sourced so exports and main()'s strict mode stay in the caller's shell.
_uctb_require_sourced() {
  case "${1:-help}" in help | -h | --help) return 0 ;; esac
  [[ "${BASH_SOURCE[0]}" != "${0}" ]] && return 0
  echo "error: run this script with source so the current shell keeps exports and options (not ./…)." >&2
  echo "  source ${BASH_SOURCE[0]} ${1}" >&2
  kill -INT $$
}

# Prefer this over : "${var:?msg}" — :? uses the shell's fatal expansion path (exit), not SIGINT (see shell-script-practices).
_uctb_require_nonempty() {
  local val=$1
  shift
  if [[ -z "${val}" ]]; then
    echo "error: $*" >&2
    kill -INT $$
  fi
}

_uctb_script_dir() {
  (cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
}

# Loads script-dir .env once per invocation (set -a) so BUCKET_NAME etc. apply even if the shell never sourced .env.
_uctb_load_dotenv_if_present() {
  local f="$(_uctb_script_dir)/.env"
  if [[ -f "${f}" && -r "${f}" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "${f}"
    set +a
    echo "loaded ${f}" >&2
  fi
}

usage() {
  cat <<'EOF'
Usage: source external_managed_location_uc.sh <discover|bucket|role>  (./… help; env.example)
  discover   metastore + exports
  bucket     S3 if missing
  role       IAM + UC_STORAGE_ROLE_ARN
EOF
}

# Args: binary names required on PATH (default: databricks jq aws).
check_prerequisites() {
  local bin
  local -a bins=("$@")
  if [[ ${#bins[@]} -eq 0 ]]; then
    bins=(databricks jq aws)
  fi
  for bin in "${bins[@]}"; do
    command -v "$bin" >/dev/null 2>&1 || {
      echo "error: '${bin}' not found (install and ensure it is on PATH)" >&2
      kill -INT $$
    }
  done
}

UCTB_UC_S3_MANUAL_URL='https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/s3/s3-external-location-manual'

# Static Principal ARNs from ${UCTB_UC_S3_MANUAL_URL} (Step 1 trust policy — commercial vs GovCloud).
_uctb_export_uc_master_from_s3_external_location_manual() {
  if [[ -n "${UC_MASTER_ACCOUNT_ID:-}" && -n "${UC_MASTER_ROLE_NAME:-}" ]]; then
    echo "UC_MASTER_ACCOUNT_ID=${UC_MASTER_ACCOUNT_ID} UC_MASTER_ROLE_NAME=${UC_MASTER_ROLE_NAME} (already set)" >&2
    return 0
  fi
  local sts_arn=''
  command -v aws >/dev/null 2>&1 && sts_arn=$(aws sts get-caller-identity --query Arn --output text 2>/dev/null) || true
  if [[ "${sts_arn}" == *arn:aws-us-gov:* ]]; then
    export UC_MASTER_ACCOUNT_ID=044793339203
    export UC_MASTER_ROLE_NAME=unity-catalog-prod-UCMasterRole-1QRFA8SGY15OJ
    echo "exported UC_MASTER_ACCOUNT_ID=${UC_MASTER_ACCOUNT_ID} UC_MASTER_ROLE_NAME=${UC_MASTER_ROLE_NAME} (GovCloud; note: DoD=170661010020/unity-catalog-prod-UCMasterRole-1DI6DL6ZP26AS — override in .env)" >&2
  else
    export UC_MASTER_ACCOUNT_ID=414351767826
    export UC_MASTER_ROLE_NAME=unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL
    echo "exported UC_MASTER_ACCOUNT_ID=${UC_MASTER_ACCOUNT_ID} UC_MASTER_ROLE_NAME=${UC_MASTER_ROLE_NAME} (per ${UCTB_UC_S3_MANUAL_URL})" >&2
  fi
}

_uctb_metastore_discover() {
  local summary cloud region
  summary=$(run_log databricks ${DATABRICKS_PROFILE:+ -p "${DATABRICKS_PROFILE}"} api get /api/2.1/unity-catalog/metastore_summary -o json)
  echo "$summary" | jq .
  read -r cloud region < <(echo "$summary" | jq -r '[(.cloud // "" | ascii_downcase), (.region // "")] | @tsv')

  unset AWS_REGION UCTB_REGION

  case "${cloud}" in
    '')
      echo "error: metastore_summary has no usable .cloud" >&2
      kill -INT $$
      ;;
    aws)
      if [[ -z "${region}" ]]; then
        echo "error: metastore_summary has no usable .region for aws" >&2
        kill -INT $$
      fi
      export UCTB_CLOUD=aws
      export UCTB_REGION="${region}"
      export AWS_REGION="${region}"
      echo "exported UCTB_CLOUD=aws UCTB_REGION=${UCTB_REGION} AWS_REGION=${AWS_REGION}" >&2
      _uctb_export_uc_master_from_s3_external_location_manual
      ;;
    azure | gcp)
      export UCTB_CLOUD="${cloud}"
      if [[ -n "${region}" ]]; then
        export UCTB_REGION="${region}"
        echo "exported UCTB_CLOUD=${UCTB_CLOUD} UCTB_REGION=${UCTB_REGION}" >&2
      else
        echo "exported UCTB_CLOUD=${UCTB_CLOUD} (no .region in metastore_summary)" >&2
      fi
      ;;
    *)
      echo "error: metastore cloud is '${cloud}'; expected aws, azure, or gcp" >&2
      kill -INT $$
      ;;
  esac

  # Default bucket: <Databricks userName before @, dots → _>_zerobus (only if BUCKET_NAME still unset after .env).
  _uctb_maybe_export_default_bucket_name
}

# current-user me → .userName local part (before @), dots → hyphens, then "${slug}-zerobus".
_uctb_maybe_export_default_bucket_name() {
  [[ -n "${BUCKET_NAME:-}" ]] && return 0
  local slug
  slug=$(databricks ${DATABRICKS_PROFILE:+ -p "${DATABRICKS_PROFILE}"} current-user me 2>/dev/null \
    | jq -r '.userName | split("@")[0] | gsub("[.]"; "-")') || return 0
  [[ -n "${slug}" ]] || return 0
  export BUCKET_NAME="${slug}-zerobus"
  echo "exported BUCKET_NAME=${BUCKET_NAME}" >&2
}

_uctb_require_metastore_discovered() {
  _uctb_require_nonempty "${UCTB_CLOUD:-}" "run source … discover first in this shell (UCTB_CLOUD is unset)"
}

cmd_bucket() {
  _uctb_require_metastore_discovered
  case "${UCTB_CLOUD}" in
    aws)
      check_prerequisites aws jq
      _uctb_bucket_aws
      ;;
    azure)
      echo "error: bucket for azure is not implemented in this script (use Terraform or az)." >&2
      kill -INT $$
      ;;
    gcp)
      echo "error: bucket for gcp is not implemented in this script (use Terraform or gcloud)." >&2
      kill -INT $$
      ;;
    *)
      echo "error: internal: UCTB_CLOUD='${UCTB_CLOUD}'" >&2
      kill -INT $$
      ;;
  esac
}

cmd_role() {
  _uctb_require_metastore_discovered
  case "${UCTB_CLOUD}" in
    aws)
      check_prerequisites aws jq
      _uctb_role_aws
      ;;
    azure)
      echo "error: role for azure is not implemented in this script (use Terraform or az)." >&2
      kill -INT $$
      ;;
    gcp)
      echo "error: role for gcp is not implemented in this script (use Terraform or gcloud)." >&2
      kill -INT $$
      ;;
    *)
      echo "error: internal: UCTB_CLOUD='${UCTB_CLOUD}'" >&2
      kill -INT $$
      ;;
  esac
}

_uctb_bucket_aws() {
  _uctb_require_nonempty "${BUCKET_NAME:-}" "set BUCKET_NAME (globally unique S3 bucket name); add it to $(_uctb_script_dir)/.env (see env.example) or export BUCKET_NAME=…"
  _uctb_require_nonempty "${AWS_REGION:-}" "set AWS_REGION (source … discover on aws, or export AWS_REGION=us-east-2)"

  if run aws s3api head-bucket --bucket "${BUCKET_NAME}" >/dev/null 2>&1; then
    echo "ok: bucket already exists s3://${BUCKET_NAME}" >&2
    _uctb_s3_browse_url_line "${BUCKET_NAME}" "${AWS_REGION}"
    return 0
  fi

  echo "creating s3://${BUCKET_NAME} in ${AWS_REGION}" >&2
  local -a create=(aws s3api create-bucket --bucket "${BUCKET_NAME}" --region "${AWS_REGION}")
  # S3 CreateBucket: omit LocationConstraint only for us-east-1 (AWS rejects it there).
  if [[ "${AWS_REGION}" != "us-east-1" ]]; then
    create+=(--create-bucket-configuration "LocationConstraint=${AWS_REGION}")
  fi
  if ! run "${create[@]}"; then
    echo "error: aws s3api create-bucket failed for s3://${BUCKET_NAME}" >&2
    kill -INT $$
  fi
  echo "ok: created s3://${BUCKET_NAME}" >&2
  _uctb_s3_browse_url_line "${BUCKET_NAME}" "${AWS_REGION}"
}

_uctb_trim_managed_suffix() {
  printf '%s' "${1:-}" | sed 's|^[[:space:]/]*||; s|[[:space:]/]*$||'
}

_uctb_role_aws() {
  _uctb_require_nonempty "${BUCKET_NAME:-}" "set BUCKET_NAME (S3 bucket for UC storage); add it to $(_uctb_script_dir)/.env (see env.example) or export BUCKET_NAME=…"
  _uctb_require_nonempty "${AWS_REGION:-}" "set AWS_REGION (source … discover on aws, or export manually)"
  _uctb_require_nonempty "${UC_MASTER_ACCOUNT_ID:-}" "set UC_MASTER_ACCOUNT_ID (UC metastore trust AWS account id)"
  _uctb_require_nonempty "${UC_MASTER_ROLE_NAME:-}" "set UC_MASTER_ROLE_NAME (UC metastore trust IAM role name)"
  IAM_ROLE_NAME="${IAM_ROLE_NAME:-${BUCKET_NAME}}"
  echo "IAM_ROLE_NAME=${IAM_ROLE_NAME}" >&2

  local path_segment bucket_arn policy_name trust_compact policy_compact
  path_segment=$(_uctb_trim_managed_suffix "${MANAGED_PATH_SUFFIX:-zerobus_ingest}")
  if [[ -z "${path_segment}" ]]; then
    echo "error: MANAGED_PATH_SUFFIX is empty after trim; set a single segment (e.g. zerobus_ingest)" >&2
    kill -INT $$
  fi

  bucket_arn="arn:aws:s3:::${BUCKET_NAME}"
  if [[ -n "${UC_TRUST_EXTERNAL_ID:-}" ]]; then
    trust_compact=$(jq -cn \
      --arg principal "arn:aws:iam::${UC_MASTER_ACCOUNT_ID}:role/${UC_MASTER_ROLE_NAME}" \
      --arg extid "${UC_TRUST_EXTERNAL_ID}" \
      '{Version:"2012-10-17",Statement:[{Effect:"Allow",Principal:{AWS:$principal},Action:"sts:AssumeRole",Condition:{StringEquals:{"sts:ExternalId":$extid}}}]}')
  else
    trust_compact=$(jq -cn \
      --arg principal "arn:aws:iam::${UC_MASTER_ACCOUNT_ID}:role/${UC_MASTER_ROLE_NAME}" \
      '{Version:"2012-10-17",Statement:[{Effect:"Allow",Principal:{AWS:$principal},Action:"sts:AssumeRole"}]}')
  fi

  policy_compact=$(jq -cn \
    --arg bucket_arn "$bucket_arn" \
    --arg p "$path_segment" \
    '{Version:"2012-10-17",Statement:[{Sid:"ListBucketWithPrefix",Effect:"Allow",Action:["s3:ListBucket","s3:GetBucketLocation"],Resource:$bucket_arn,Condition:{StringLike:{"s3:prefix":[$p,($p+"/*")]}}},{Sid:"ObjectRWUnderPrefix",Effect:"Allow",Action:["s3:GetObject","s3:PutObject","s3:DeleteObject","s3:AbortMultipartUpload"],Resource:($bucket_arn+"/"+$p+"/*")}]}')

  policy_name="${IAM_ROLE_NAME}-s3-uc-prefix"

  if run aws iam get-role --role-name "${IAM_ROLE_NAME}" >/dev/null 2>&1; then
    echo "ok: iam role already exists ${IAM_ROLE_NAME}" >&2
    run aws iam update-assume-role-policy --role-name "${IAM_ROLE_NAME}" --policy-document "${trust_compact}" \
      || { echo "error: aws iam update-assume-role-policy failed (check iam:UpdateAssumeRolePolicy permission)" >&2; kill -INT $$; }
  else
    echo "creating iam role ${IAM_ROLE_NAME} (trust UC principal arn:aws:iam::${UC_MASTER_ACCOUNT_ID}:role/${UC_MASTER_ROLE_NAME})" >&2
    run aws iam create-role --role-name "${IAM_ROLE_NAME}" --assume-role-policy-document "${trust_compact}" \
      || { echo "error: aws iam create-role failed (check iam:CreateRole permission)" >&2; kill -INT $$; }
  fi

  run aws iam put-role-policy --role-name "${IAM_ROLE_NAME}" --policy-name "${policy_name}" --policy-document "${policy_compact}" \
    || { echo "error: aws iam put-role-policy failed (check iam:PutRolePolicy permission)" >&2; kill -INT $$; }
  echo "ok: inline policy ${policy_name} on role ${IAM_ROLE_NAME} (prefix s3://${BUCKET_NAME}/${path_segment}/)" >&2

  UC_STORAGE_ROLE_ARN=$(aws iam get-role --role-name "${IAM_ROLE_NAME}" --query 'Role.Arn' --output text) \
    || { echo "error: aws iam get-role failed after create/update" >&2; kill -INT $$; }
  export UC_STORAGE_ROLE_ARN
  echo "exported UC_STORAGE_ROLE_ARN=${UC_STORAGE_ROLE_ARN}" >&2
}

main() {
  # When sourced, enable strict mode for this function only, then restore caller options on return.
  if [[ "${BASH_SOURCE[0]}" != "${0}" ]]; then
    set -euo pipefail
    trap 'set +e; set +u; set +o pipefail 2>/dev/null || true; trap - RETURN' RETURN
  fi
  local cmd=${1:-help}
  _uctb_require_sourced "${cmd}"
  case "${cmd}" in help | -h | --help) ;;
    *) _uctb_load_dotenv_if_present ;;
  esac
  case "${cmd}" in
    discover)
      check_prerequisites databricks jq
      _uctb_metastore_discover
      ;;
    bucket)
      cmd_bucket
      ;;
    role)
      cmd_role
      ;;
    help | -h | --help) usage ;;
    *) echo "unknown command: $cmd" >&2; usage >&2; kill -INT $$ ;;
  esac
}

main "$@"
