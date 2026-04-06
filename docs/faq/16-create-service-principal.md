# How do I create a Service Principal for ZeroBus authentication?

`ZEROBUS_APP_ID` and `ZEROBUS_OAUTH_SECRET` are OAuth M2M credentials for a Databricks Service Principal. Creating them requires workspace admin.

> Replace `DEFAULT` with your profile name if using a named profile in `~/.databrickscfg`.

---

## Step 1 — Create the Service Principal

```bash
SP_JSON=$(databricks service-principals create \
  --display-name "robert_lee_zerobus_sp" \
  --json '{}' \
  --profile DEFAULT \
  --output json)

APP_ID=$(echo "$SP_JSON" | jq -r '.applicationId')
SP_ID=$(echo "$SP_JSON" | jq -r '.id')

echo "ZEROBUS_APP_ID: $APP_ID"
echo "ZEROBUS_SERVICE_PRINCIPAL_ID (numeric, needed for step 2): $SP_ID"
```

`applicationId` is a UUID — this is `ZEROBUS_APP_ID`, passed to `create_stream`.
`id` is a numeric value used only to create the secret in step 2.

> **Note:** display names are not unique. Multiple SPs can share the same name.
> Use `ZEROBUS_APP_ID` (the UUID) as the stable identifier.

---

## Step 2 — Create the OAuth secret

```bash
OAUTH_SECRET=$(databricks service-principal-secrets-proxy create $SP_ID \
  --profile DEFAULT \
  --output json | jq -r '.secret')

echo "ZEROBUS_OAUTH_SECRET: $OAUTH_SECRET"
```

The `secret` value is returned **once**. Store it before closing the terminal.

> **Quota:** each SP is limited to **5 OAuth secrets**. Delete unused secrets with:
> ```bash
> databricks service-principal-secrets-proxy list $SP_ID --output json
> databricks service-principal-secrets-proxy delete $SP_ID <secret_id>
> ```

---

## Combined (steps 1 and 2 together)

```bash
SP_JSON=$(databricks service-principals create \
  --display-name "robert_lee_zerobus_sp" \
  --json '{}' \
  --profile DEFAULT \
  --output json)

APP_ID=$(echo "$SP_JSON" | jq -r '.applicationId')
SP_ID=$(echo "$SP_JSON" | jq -r '.id')

# Fallback: look up SP_ID from APP_ID if create returned empty id
if [[ -z "$SP_ID" || "$SP_ID" == "null" ]]; then
    SP_ID=$(databricks service-principals list \
      --filter "applicationId eq $APP_ID" \
      --output json | jq -r '.[0].id')
fi

OAUTH_SECRET=$(databricks service-principal-secrets-proxy create "$SP_ID" \
  --profile DEFAULT \
  --output json | jq -r '.secret')

echo "ZEROBUS_SERVICE_PRINCIPAL_ID=$SP_ID"
echo "ZEROBUS_APP_ID=$APP_ID"
echo "ZEROBUS_OAUTH_SECRET=$OAUTH_SECRET"

# Write to config.json — creates the file if it does not exist
CONFIG_FILE="config.json"
EXISTING=$(cat "$CONFIG_FILE" 2>/dev/null || echo '{}')
echo "$EXISTING" \
  | jq --arg sp  "$SP_ID" \
       --arg id  "$APP_ID" \
       --arg sec "$OAUTH_SECRET" \
       '.ZEROBUS_SERVICE_PRINCIPAL_ID=$sp | .ZEROBUS_APP_ID=$id | .ZEROBUS_OAUTH_SECRET=$sec' \
  > "$CONFIG_FILE"
echo "Written to $CONFIG_FILE"
```

---

## Grant table permissions

The target table must exist before permissions can be granted. Create schema and table first if needed:

```sql
CREATE SCHEMA IF NOT EXISTS <catalog.schema>;
CREATE TABLE IF NOT EXISTS <catalog.schema.table> (
  -- columns matching your ingest schema
);
```

Then grant the required permissions to the Service Principal:

```sql
GRANT USE CATALOG ON CATALOG <catalog>          TO `<APP_ID>`;
GRANT MODIFY, SELECT ON TABLE <catalog.schema.table> TO `<APP_ID>`;
```

**CLI equivalent:**
```bash
databricks grants update TABLE <catalog.schema.table> \
  --profile DEFAULT \
  --json '{
    "changes": [{
      "principal": "<APP_ID>",
      "add": ["MODIFY", "SELECT"]
    }]
  }'
```

---

## Validate credentials

Before using in the notebook, confirm the credentials work against the OIDC token endpoint:

```bash
curl -s -X POST https://<workspace-url>/oidc/v1/token \
  -d "grant_type=client_credentials" \
  -d "client_id=$APP_ID" \
  -d "client_secret=$OAUTH_SECRET" \
  -d "scope=all-apis" | jq .
```

A `200` response with `access_token` confirms the credentials are valid.

---

## config.json keys

| Key | Description |
|-----|-------------|
| `ZEROBUS_APP_ID` | UUID `applicationId` — passed as `client_id` to `create_stream` |
| `ZEROBUS_OAUTH_SECRET` | OAuth secret — passed as `client_secret` to `create_stream` |
| `ZEROBUS_SERVICE_PRINCIPAL_ID` | Numeric SP resource ID — used to manage secrets |

*[← Back to FAQ index](../zerobus_faq.md)*
