---
name: databricks-asset-bundle-sync
description: Configure databricks.yml to sync only specific files to Databricks, optionally with versioned workspace paths. Use when the user wants to limit Databricks Asset Bundle (DAB) sync, restrict which files are uploaded, fix a databricks.yml that is syncing too many files, or version the upload destination path.
---

# Databricks Asset Bundle Sync

## Key Rules

1. `sync` must be at the **top level** of `databricks.yml` — NOT nested under `bundle`
2. Use `sync.paths` as an explicit allowlist — it syncs only what is listed
3. Avoid `sync.include` + `sync.exclude: ["**"]` — excludes override includes and the config is silently ignored

## GCP workspaces — `workspace.host` in `databricks.yml`

GCP workspace URLs are usually **numeric**:

`https://<WORKSPACE_ID>.<shard>.gcp.databricks.com`  
(example: `https://8259565162423548.8.gcp.databricks.com`)

Set that value under `targets.<name>.workspace.host` for the bundle target you use with the **Databricks VS Code extension** and for reliable **`databricks bundle sync`**. The URL is **not** a secret (workspace id is public to users of that workspace); keep OAuth tokens only in `~/.databrickscfg`.

**When `host` might be omitted:** `databricks bundle` can sometimes resolve the workspace from `workspace.profile` alone if the named `[profile]` in `~/.databrickscfg` already defines `host`. The extension and some setups **still require** `host` in YAML for GCP—if sync or “pick workspace” fails, add explicit `host` (match the host you used for `databricks auth login --host …`).

**Not a substitute for notebooks:** Python / SDK / Connect use `~/.databrickscfg` (or env vars). `databricks.yml` `host` is for **bundle / IDE** workspace binding, not for `WorkspaceClient()` in repo code.

## Template (with versioned path)

```yaml
bundle:
  name: <bundle-name>

variables:
  version:
    default: "0.0.1"
    description: "Notebook bundle version — bump this when releasing a new version"

sync:
  paths:
    - path/to/notebook.ipynb
    - path/to/another.ipynb  # add more as needed

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://<workspace-url>
      file_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/v${var.version}/files
```

## Versioning Behaviour

- Each version syncs to its **own path** — old versions are never deleted
- Bumping `version` abandons the old path and creates a new one
- To keep `databricks.yml` in sync with the pylib version automatically, add this to `bump_version.py`'s `write_version()`:

```python
databricks_yml = base_dir.parent / "databricks.yml"
if databricks_yml.exists():
    content = databricks_yml.read_text()
    content = re.sub(
        r'(version:\s+default:\s+")[^"]+(")',
        rf'\g<1>{version}\2',
        content,
    )
    databricks_yml.write_text(content)
```

## Steps

1. Read the existing `databricks.yml`
2. Remove any `sync` block nested under `bundle`
3. Add a top-level `variables.version` block with the current version
4. Add a top-level `sync.paths` block listing only the notebooks to sync
5. Add `file_path` with `v${var.version}` to the workspace target
6. Trigger a sync and verify the output shows only the listed files

## Validation

After sync, the terminal output should show:
```
Starting synchronization (N files)
Uploaded path/to/notebook.ipynb
Completed synchronization
```

If you still see `Warning: unknown field: sync at bundle`, the `sync` block is still nested under `bundle` — move it to the top level.
