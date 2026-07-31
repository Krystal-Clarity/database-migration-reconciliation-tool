---
name: databricks-wheel-publisher
description: Build and publish this project's Python wheel to Databricks for notebook testing. Use this skill whenever the user says or implies "upload this to Databricks", "put the wheel in Databricks", "make a wheel", "build the wheel", "upload the new wheel", "replace the old whl", "refresh the Databricks package", "deploy the latest code", or "make my notebook use the latest version". Also use it for requests mentioning a .whl, %pip install --force-reinstall, Databricks workspace files, or replacing an existing Databricks wheel, even when the user does not explicitly say "publish" or "deploy".
compatibility: Requires the Databricks CLI, uv, an authenticated Databricks OAuth profile, and this repository's pyproject.toml.
---

# Publish the project wheel to Databricks

Use this workflow for the notebook install path:

```python
%pip install --force-reinstall --no-deps "/Workspace/Users/angus.blance@krystalclarity.com/data-reconciliation-tool/database_migration_reconciliation_tool-0.1.0-py3-none-any.whl"
```

The remote artifact is a Databricks workspace file. Keep the workspace path unchanged unless the user supplies a replacement.

## Constants

```bash
PROFILE="DEFAULT"
WORKSPACE_WHEEL="/Workspace/Users/angus.blance@krystalclarity.com/data-reconciliation-tool/database_migration_reconciliation_tool-0.1.0-py3-none-any.whl"
LOCAL_WHEEL="dist/database_migration_reconciliation_tool-0.1.0-py3-none-any.whl"
```

Always pass `--profile DEFAULT` to Databricks commands. This is more reliable than depending on shell environment variables.

## Authenticate before uploading

Check the profile first:

```bash
databricks auth profiles
databricks current-user me --profile DEFAULT
```

If `DEFAULT` is invalid, stop and ask the user to complete OAuth login:

```bash
databricks auth login \
  --host "https://adb-3990188733873176.16.azuredatabricks.net" \
  --profile DEFAULT
```

Use OAuth only. Never request, print, store, or recommend a personal access token. The local OAuth profile can refresh over multiple days, subject to organizational session policy.

## Build a fresh wheel

Clean only generated build outputs so stale files cannot be packaged:

```bash
rm -rf build dist
uv build --wheel
```

Verify the artifact and, after PK comparison changes, verify that it contains the new implementation:

```bash
unzip -l "$LOCAL_WHEEL"
unzip -p "$LOCAL_WHEEL" column_comparison/column_comparison.py | rg -n "Exclusive PKs|DataFrame 1|Joined DataFrames"
```

The package mapping in `pyproject.toml` packages `src/database-migration-reconciliation-tool` as `column_comparison`, including `index.html` and `styles.css`. If the wheel does not contain current source markers, do not upload it; clean and rebuild.

## Replace the workspace wheel

Use overwrite in place. Do not delete the remote file first because that creates an unnecessary gap for notebook users:

```bash
databricks workspace import "$WORKSPACE_WHEEL" \
  --file "$LOCAL_WHEEL" \
  --format RAW \
  --overwrite \
  --profile DEFAULT
```

If overwrite fails, inspect the exact target:

```bash
databricks workspace get-status "$WORKSPACE_WHEEL" --profile DEFAULT
```

Only if the target is the expected wheel and overwrite is unavailable, delete that exact file and import again:

```bash
databricks workspace delete "$WORKSPACE_WHEEL" --profile DEFAULT
databricks workspace import "$WORKSPACE_WHEEL" \
  --file "$LOCAL_WHEEL" \
  --format RAW \
  --profile DEFAULT
```

Never use recursive deletion or a broad workspace path.

## Verify and hand off

Confirm the replacement:

```bash
databricks workspace get-status "$WORKSPACE_WHEEL" --profile DEFAULT
```

Report the path, object type, modified time, and size. Tell the user to rerun their notebook `%pip install --force-reinstall --no-deps` cell and restart Python if Databricks requests it; uploading does not update an already-running Python process.

## Completion checklist

- Authentication was checked before upload.
- A fresh wheel was built after cleaning only generated `build/` and `dist/`.
- The wheel contains current source and package assets.
- The exact workspace wheel path was overwritten.
- `workspace get-status` confirmed the replacement.
- The user was told to reinstall the wheel and restart Python if needed.
