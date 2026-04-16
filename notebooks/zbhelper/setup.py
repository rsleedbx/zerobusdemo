"""
zbhelper.setup — reusable Step 2 helpers for ZeroBus demo notebooks.

Three functions cover all of Step 2 (a/b/c/d) and Step 3:

    discover_workspace()          → workspace URL, ID, region, ZeroBus endpoints
    ensure_service_principal()    → SP exists, OAuth secret valid/minted
    ensure_table()                → UC table created, SP granted

Intended usage (benchmark driver or any notebook)::

    import zbhelper.setup as zbsetup

    ws  = zbsetup.discover_workspace(dbutils)
    sp  = zbsetup.ensure_service_principal(ws, dbutils, sp_name="lfcdemo_zerobus")
    tbl = zbsetup.ensure_table(spark, ws, sp["client_id"], table="airquality_benchmark")

    SERVER_ENDPOINT           = ws["server_endpoint"]
    DATABRICKS_WORKSPACE_URL  = ws["workspace_url"]
    DATABRICKS_WORKSPACE_ID   = ws["workspace_id"]
    ZEROBUS_INGEST_URL        = ws["zerobus_ingest_url"]
    CLIENT_ID                 = sp["client_id"]
    CLIENT_SECRET             = sp["client_secret"]
    CATALOG, SCHEMA, TABLE_NAME = tbl["catalog"], tbl["schema"], tbl["table_name"]
"""

import json
import os
import re
import urllib.error
import urllib.request
from typing import Any
from urllib.parse import urlencode

# ── constants ──────────────────────────────────────────────────────────────────

_CLOUD_DOMAIN = {
    "AWS": "cloud.databricks.com",
    "AZURE": "azuredatabricks.net",
    "GCP": "gcp.databricks.com",
}

_DEFAULT_SP_SECRET_SCOPE = "lfczerobusdemo"
_DEFAULT_SP_SECRET_KEY   = "lfczerobusdemo"
_DEFAULT_OAUTH_JSON_FIELD = "ZEROBUS_OAUTH_SECRET"

_ZB_EXCLUDED_CATALOGS = frozenset({"", "hive_metastore", "spark_catalog"})


# ── Step 2.a ───────────────────────────────────────────────────────────────────

def discover_workspace(dbutils=None, region_override: str = "") -> dict:
    """Step 2.a: auto-discover workspace URL, numeric ID, region, and ZeroBus endpoints.

    Clears metadata-service auth env vars so WorkspaceClient() uses ~/.databrickscfg
    when called from a Databricks Connect (local) kernel.

    Returns a dict with keys:
        workspace_url, workspace_id, region, server_endpoint, zerobus_ingest_url,
        sp_ui_prefix, o_query, client (WorkspaceClient instance)
    """
    from databricks.sdk import WorkspaceClient

    if (os.environ.get("DATABRICKS_AUTH_TYPE") or "").strip().lower() == "metadata-service":
        os.environ.pop("DATABRICKS_AUTH_TYPE", None)
        os.environ.pop("DATABRICKS_METADATA_SERVICE_URL", None)
        print("Cleared metadata-service auth env vars (Connect kernel).")

    w = WorkspaceClient()
    workspace_url = w.config.host.rstrip("/")
    workspace_id  = str(w.get_workspace_id())

    summary = w.metastores.summary()
    region  = (region_override or "").strip() or summary.region
    domain  = _CLOUD_DOMAIN.get((summary.cloud or "").upper())
    if not region or not domain:
        raise RuntimeError(
            f"Cannot determine ZeroBus endpoint: cloud={summary.cloud!r} region={region!r}. "
            "Set region_override explicitly."
        )

    zerobus_ingest_url = f"https://{workspace_id}.zerobus.{region}.{domain}"
    server_endpoint    = zerobus_ingest_url

    print(
        f"workspace_url={workspace_url}\n"
        f"workspace_id={workspace_id}\n"
        f"region={region}\n"
        f"server_endpoint={server_endpoint}"
    )
    return {
        "workspace_url":    workspace_url,
        "workspace_id":     workspace_id,
        "region":           region,
        "server_endpoint":  server_endpoint,
        "zerobus_ingest_url": zerobus_ingest_url,
        "sp_ui_prefix":     f"{workspace_url}/settings/workspace/identity-and-access/service-principals",
        "o_query":          f"?o={workspace_id}",
        "client":           w,
    }


# ── Step 2.b + 2.c ────────────────────────────────────────────────────────────

def ensure_service_principal(
    ws: dict,
    dbutils,
    sp_name: str,
    secret_scope: str | None = None,
    secret_key:   str | None = None,
    oauth_field:  str | None = None,
) -> dict:
    """Steps 2.b + 2.c: ensure the service principal exists and the OAuth secret is valid.

    Parses sp_name (supports prefix / prefix--scope / prefix--scope--key /
    prefix--scope--key--field formats). Creates the secret scope and SP if missing.
    Mints a new OAuth client secret if the stored one is missing or fails OIDC validation.

    Returns a dict with keys:
        client_id, client_secret, sp_id, secret_scope, secret_key, oauth_field
    """
    from databricks.sdk.errors import NotFound, ResourceAlreadyExists, ResourceDoesNotExist

    w            = ws["client"]
    workspace_url = ws["workspace_url"]

    scope, key, field = _parse_secret_ref(sp_name, secret_scope, secret_key, oauth_field)

    # ── helper closures ───────────────────────────────────────────────────────

    def _load_blob() -> dict:
        try:
            raw = dbutils.secrets.get(scope=scope, key=key)
            return json.loads(raw)
        except Exception:
            return {}

    def _save_blob(blob: dict) -> None:
        w.secrets.put_secret(scope=scope, key=key, string_value=json.dumps(blob, indent=2))

    def _ensure_scope_and_key() -> None:
        try:
            w.secrets.create_scope(scope)
            print(f"Created secret scope {scope!r}")
        except ResourceAlreadyExists:
            pass
        try:
            w.secrets.get_secret(scope, key)
        except ResourceDoesNotExist:
            w.secrets.put_secret(scope=scope, key=key, string_value="{}")
            print(f"Created empty secret {scope!r}/{key!r}")

    # ── Step 2.b: service principal ───────────────────────────────────────────

    _ensure_scope_and_key()
    blob = _load_blob()

    sp_id  = str(blob.get("ZEROBUS_SERVICE_PRINCIPAL_ID") or "").strip()
    app_id = str(blob.get("ZEROBUS_APP_ID") or "").strip()

    if not sp_id:
        print("ZEROBUS_SERVICE_PRINCIPAL_ID not set — creating service principal")
        sp     = w.service_principals.create(display_name=sp_name)
        sp_id  = str(sp.id)
        app_id = str(sp.application_id)
        print(f"Created SP {sp_name!r} sp_id={sp_id} app_id={app_id}")
    else:
        try:
            sp = w.service_principals.get(sp_id)
            api_app = str(sp.application_id)
            if app_id and app_id != api_app:
                raise RuntimeError(
                    f"Stored ZEROBUS_APP_ID={app_id!r} does not match workspace "
                    f"application_id={api_app!r} for sp_id={sp_id!r}."
                )
            app_id = api_app
            print(f"SP exists: {sp.display_name!r} sp_id={sp_id}")
        except NotFound:
            print(f"SP id={sp_id!r} not found — creating replacement")
            sp     = w.service_principals.create(display_name=sp_name)
            sp_id  = str(sp.id)
            app_id = str(sp.application_id)
            print(f"Created replacement SP sp_id={sp_id} app_id={app_id}")

    blob.update({
        "ZEROBUS_SERVICE_PRINCIPAL_NAME": sp_name,
        "ZEROBUS_SERVICE_PRINCIPAL_ID":   sp_id,
        "ZEROBUS_APP_ID":                 app_id,
    })
    _save_blob(blob)

    # ── Step 2.c: OAuth client secret ─────────────────────────────────────────

    client_secret = str(blob.get(field) or "").strip()

    if not _oidc_valid(app_id, client_secret, workspace_url):
        print("OAuth client secret missing or invalid — minting a new one…")
        client_secret = _mint_client_secret(w, sp_id)
        if not _oidc_valid(app_id, client_secret, workspace_url):
            raise RuntimeError("New client secret failed OIDC validation.")
        blob[field] = client_secret
        _save_blob(blob)
        print(f"Saved new OAuth secret to {scope!r}/{key!r} field {field!r}")
    else:
        print("OAuth client secret is valid.")

    return {
        "client_id":     app_id,
        "client_secret": client_secret,
        "sp_id":         sp_id,
        "secret_scope":  scope,
        "secret_key":    key,
        "oauth_field":   field,
    }


# ── Step 2.d + Step 3 ─────────────────────────────────────────────────────────

def ensure_table(
    spark,
    ws: dict,
    client_id: str,
    catalog: str = "",
    schema:  str = "",
    table:   str = "airquality",
) -> dict:
    """Step 2.d + Step 3: resolve catalog/schema, create UC managed table, grant SP permissions.

    Returns a dict with keys: catalog, schema, table, table_name
    """
    w = ws["client"]

    if not catalog.strip():
        catalog = _default_catalog(spark)
        print(f"CATALOG auto-detected: {catalog!r}")

    if not schema.strip():
        schema = re.sub(r"[^a-z0-9]", "_", w.current_user.me().user_name.split("@")[0].lower())
        print(f"SCHEMA auto-detected: {schema!r}")

    table_name = f"{catalog}.{schema}.{table}"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            device_name STRING,
            temp        INT,
            humidity    BIGINT
        ) USING DELTA
    """)
    spark.sql(f"GRANT USE CATALOG ON CATALOG {catalog} TO `{client_id}`;").collect()
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema} TO `{client_id}`;").collect()
    spark.sql(f"GRANT MODIFY, SELECT ON TABLE {table_name} TO `{client_id}`;").collect()

    print(f"Table ready: {table_name}")
    return {"catalog": catalog, "schema": schema, "table": table, "table_name": table_name}


# ── private helpers ────────────────────────────────────────────────────────────

def _parse_secret_ref(
    sp_name: str,
    secret_scope: str | None,
    secret_key:   str | None,
    oauth_field:  str | None,
) -> tuple[str, str, str]:
    """Parse scope/key/field from sp_name or explicit overrides."""
    if secret_scope and secret_key and oauth_field:
        return secret_scope, secret_key, oauth_field

    parts = [p for p in str(sp_name).strip().split("--") if p]
    if not parts:
        raise ValueError("sp_name is empty.")
    if len(parts) == 1:
        scope, key, field = _DEFAULT_SP_SECRET_SCOPE, _DEFAULT_SP_SECRET_KEY, _DEFAULT_OAUTH_JSON_FIELD
    elif len(parts) == 2:
        scope, key, field = parts[1], _DEFAULT_SP_SECRET_KEY, _DEFAULT_OAUTH_JSON_FIELD
    elif len(parts) == 3:
        if re.fullmatch(r"[A-Z][A-Z0-9_]*", parts[2]):
            scope, key, field = parts[0], parts[1], parts[2]
        else:
            scope, key, field = parts[1], parts[2], _DEFAULT_OAUTH_JSON_FIELD
    elif len(parts) == 4:
        scope, key, field = parts[1], parts[2], parts[3]
    else:
        raise ValueError(f"sp_name has too many '--' segments: {sp_name!r}")

    return (
        secret_scope or scope,
        secret_key   or key,
        oauth_field  or field,
    )


def _oidc_valid(client_id: str, client_secret: str, workspace_url: str) -> bool:
    if not client_id or not client_secret:
        return False
    try:
        body = urlencode({
            "grant_type":    "client_credentials",
            "client_id":     client_id,
            "client_secret": client_secret,
            "scope":         "all-apis",
        }).encode()
        req = urllib.request.Request(
            f"{workspace_url.rstrip('/')}/oidc/v1/token",
            data=body, method="POST",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        with urllib.request.urlopen(req, timeout=15) as r:
            return 200 <= getattr(r, "status", 200) < 300
    except urllib.error.HTTPError as ex:
        print(f"OIDC check: HTTP {ex.code}")
        return False
    except Exception as ex:
        print(f"OIDC check: {ex}")
        return False


def _mint_client_secret(w: Any, sp_id: str) -> str:
    try:
        obj = w.service_principal_secrets_proxy.create(service_principal_id=sp_id)
        return obj.secret
    except AttributeError:
        pass
    try:
        from databricks.sdk import ServicePrincipalSecretsAPI
        obj = ServicePrincipalSecretsAPI(w.api_client).create(service_principal_id=sp_id)
        return obj.secret
    except Exception:
        pass
    resp = w.api_client.do("POST", f"/api/2.0/accounts/servicePrincipals/{sp_id}/credentials/secrets")
    return resp["secret"]


def _default_catalog(spark) -> str:
    cur = spark.sql("SELECT current_catalog()").collect()[0][0]
    if str(cur).strip().lower() not in _ZB_EXCLUDED_CATALOGS:
        return str(cur)
    names = [
        str(r[0]).strip()
        for r in spark.sql("SHOW CATALOGS").collect()
        if str(r[0]).strip().lower() not in _ZB_EXCLUDED_CATALOGS
    ]
    if not names:
        raise RuntimeError(
            f"No Unity Catalog catalog found (current={cur!r}). Set catalog explicitly."
        )
    main = next((n for n in names if n.lower() == "main"), None)
    chosen = main or sorted(names, key=str.lower)[0]
    print(f"current_catalog was {cur!r}; using {chosen!r}")
    return chosen
