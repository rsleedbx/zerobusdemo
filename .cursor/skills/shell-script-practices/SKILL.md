---
name: shell-script-practices
description: Bash conventions for this repo — run() / run_log() ("$@" never "${*}"); abort with kill -INT $$; prerequisites for-bin + command -v loop; ${VAR:?msg}, ${VAR:+ …}, argv arrays + run "${cmd[@]}", inline single-command bodies. Use when writing or refactoring .sh scripts, CLI wrappers, or small automation next to Python/Terraform.
---

# Shell script practices

## Abort: prefer `kill -INT $$` over `exit 1` for manual errors

- After printing an error for a **recoverable / user** failure (missing tool, bad command, failed `aws`), use **`kill -INT $$`** instead of **`exit 1`**.
- **Why:** If the file is ever **`source`d** into an interactive shell, **`exit 1` closes that shell**; **`kill -INT $$`** sends **SIGINT** to the current process (usually “cancelled” behavior: stop the script path without logging out of the session).
- When the script is run as **`./script.sh`**, both stop the script process; exit status for SIGINT is typically **130** (`128 + 2`).
- **`${var:?msg}`** and **`set -e`** still use the shell’s own fatal paths; this rule targets **explicit** `exit 1` after `echo` / checks.

```bash
if ! "${create[@]}"; then
  echo "error: create-bucket failed" >&2
  kill -INT $$
fi
```

## Check prerequisites once

- Probe **required** external tools **one time** before real work (e.g. one `check_prerequisites` from `main` for every subcommand that needs them), not inside every `if`/`case` branch.
- **One global prerequisite function** for the script when subcommands share the same toolchain (e.g. `databricks`, `jq`, `aws` together). Avoid a separate `check_prerequisites_foo` per subcommand unless a command truly needs a **disjoint** set of binaries.
- **Prefer required dependencies** over optional ones: list them in the script header and `usage`, fail fast if missing, and **avoid** parallel code paths that differ only because a formatter or helper might be absent.

### Canonical loop: `command -v` over a fixed tool list

- Put every required binary name in **one** `for … in …` list—adding a tool means editing **one** line.
- Use **`command -v "$bin"`** (POSIX-friendly) to detect presence; redirect stdout/stderr to `/dev/null` so the check stays quiet.
- On first missing tool: **message to stderr**, then **`kill -INT $$`** (see “Abort” above)—no copy-pasted `if ! command -v databricks` / `if ! command -v jq` blocks.

```bash
check_prerequisites() {
  local bin
  for bin in databricks jq aws; do
    command -v "$bin" >/dev/null 2>&1 || {
      echo "error: '${bin}' not found (install and ensure it is on PATH)" >&2
      kill -INT $$
    }
  done
}
```

**Repo example:** `scripts/uctblextstorage/external_managed_location_uc.sh` — `check_prerequisites databricks jq` for **`discover`**; **`check_prerequisites aws jq`** inside the **`bucket`** / **`role`** arms when **`UCTB_CLOUD`** is **`aws`**.

## Avoid optional-tool branching

- Do not add `if has_jq; then … else …` (or similar) when requiring the tool is acceptable; one path is easier to read and test.
- If a dependency is truly optional for a rare mode, isolate that in a subcommand or separate script so the default path stays single-track.

## Structure

- `usage` / `help` paths should **not** invoke tools that real commands need (only print text).
- Shared API calls live in one helper (`_foo`); branching only on profile/env, not on “is formatter installed.”

## Inline a single command

- Do **not** add `cmd_foo() { only_one_pipeline; }` when `foo` is used once and the body is a **single** pipeline or invocation—put that line **inline** in `main`’s `case` arm (or next to the sole caller).
- Keep a **named function** when the body is non-trivial, reused from multiple sites, or you need a stable name for testing/documentation.

## Required environment variables (avoid `: "${var:?msg}"` when you want SIGINT)

**`${parameter:?word}`** on unset/null writes `word` to stderr and uses the shell’s **fatal expansion** path (often **exit 1**, not SIGINT). That is fine in many **executed** scripts; for **`source`d** flows where you want the same **“cancelled”** behavior as other manual errors, **do not** rely on `:?` alone.

Prefer a tiny helper that **`echo`s to stderr** and then **`kill -INT $$`**:

```bash
_uctb_require_nonempty() {
  local val=$1
  shift
  if [[ -z "${val}" ]]; then
    echo "error: $*" >&2
    kill -INT $$
  fi
}
```

```bash
_uctb_bucket_aws() {
  _uctb_require_nonempty "${BUCKET_NAME:-}" "set BUCKET_NAME (globally unique S3 bucket name)"
  _uctb_require_nonempty "${AWS_REGION:-}" "set AWS_REGION (e.g. from discover .region)"
  …
}
```

**Repo example:** `scripts/uctblextstorage/external_managed_location_uc.sh` (`_uctb_require_nonempty`, `_uctb_bucket_aws`, `_uctb_role_aws`).

## Optional CLI flags without verbose `if`/`else`

When a flag (e.g. `-p profile`) is **omitted when unset** and **passed when set**, use **one** command line instead of duplicating the whole invocation.

- Use **`${parameter:+word}`** (if *set and non-empty*, substitute `word`; otherwise nothing). Do **not** use **`:-`** here—that chooses a default when unset, which is the wrong semantics for “optional flag.”
- Put a **leading space** inside `word` so the previous token and the flag stay separate words (e.g. `databricks` then `-p`, not `databricks-p`).
- **Quote** the variable inside `word` so values with spaces stay a single argument.

```bash
# One line; no duplicated databricks … api get …
databricks ${DATABRICKS_PROFILE:+ -p "${DATABRICKS_PROFILE}"} api get "${path}" -o json
```

Same pattern for other tools: optional `--region`, `--namespace`, etc., when unset means “use tool default.”

**Repo example:** `scripts/uctblextstorage/external_managed_location_uc.sh` (`_uctb_metastore_discover`).

## `run() { "$@"; }` for a static argv (not `"${*}"`)

- A one-line wrapper **`run() { "$@"; }`** runs the caller’s arguments as a normal command with **word boundaries preserved**.
- Do **not** use **`"${*}"`**: it joins parameters into **one** string (IFS-separated), so `run aws s3api …` becomes a **single** unusable token.
- Use **`run aws …`** for fixed commands; use an **argv array** when you **`+=`** optional flags, then **`run "${cmd[@]}"`**.

```bash
run() { "$@"; }

if run aws s3api head-bucket --bucket "${BUCKET_NAME}" >/dev/null 2>&1; then
  echo "already exists"
  return 0
fi
```

**Repo example:** `scripts/uctblextstorage/external_managed_location_uc.sh` (`run`, `_uctb_bucket_aws` head check).

## `run_log`: print argv then run

- When you would **`echo … >&2`** immediately before the same **`"$@"`** invocation, use **`run_log`** instead: **`printf '%q ' "$@"`** on stderr (re-executable word list), newline, then **`"$@"`**.
- Keeps **`run`** silent for checks (e.g. `head-bucket` to `/dev/null`); use **`run_log`** only where stderr tracing is intended.

```bash
run_log() {
  printf '%q ' "$@" >&2
  echo >&2
  "$@"
}
```

**Repo example:** `scripts/uctblextstorage/external_managed_location_uc.sh` (`run_log`, `_uctb_metastore_discover`).

## Build a CLI as an argv array (`"${cmd[@]}"`)

- Declare **`local -a cmd=(program fixed-arg …)`** for the stable prefix, then **`cmd+=(extra args)`** for conditional flags or values.
- Execute with **`run "${cmd[@]}"`** (or **`"${cmd[@]}"`** alone) so each array element is one argument (safe for spaces; no `eval`, no fragile string concat).
- Prefer this over two near-duplicate full command lines when only a tail segment differs.

```bash
local -a create=(aws s3api create-bucket --bucket "${BUCKET_NAME}" --region "${AWS_REGION}")
[[ "${AWS_REGION}" != "us-east-1" ]] && create+=(--create-bucket-configuration "LocationConstraint=${AWS_REGION}")
if ! run "${create[@]}"; then
  echo "error: create-bucket failed" >&2
  kill -INT $$
fi
echo "ok: created"
```

**Repo example:** `scripts/uctblextstorage/external_managed_location_uc.sh` (`_uctb_bucket_aws`).

## Example shape

```bash
run() { "$@"; }
run_log() {
  printf '%q ' "$@" >&2
  echo >&2
  "$@"
}

check_prerequisites() {
  local bin
  for bin in databricks jq aws; do
    command -v "$bin" >/dev/null 2>&1 || {
      echo "error: '${bin}' not found (install and ensure it is on PATH)" >&2
      kill -INT $$
    }
  done
}

main() {
  case "${1:-help}" in
    discover)
      check_prerequisites
      _metastore_summary_json
      ;;
    discover-create-s3-bucket)
      check_prerequisites
      _metastore_summary_json
      cmd_create_s3_bucket
      ;;
    create-s3-bucket)
      check_prerequisites
      cmd_create_s3_bucket
      ;;
    …
  esac
}
```

**Repo example:** `scripts/uctblextstorage/external_managed_location_uc.sh` — **`discover`** must be **`source`d** (`BASH_SOURCE` vs `$0` guard); **`discover-create-s3-bucket`** runs `_metastore_summary_json` then **`cmd_create_s3_bucket`** in one **`./`** process; shared `check_prerequisites`.
