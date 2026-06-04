# Security

## Credentials management

All secrets are injected via environment variables. Never hardcode them.

| Variable | Where to set | Notes |
|----------|-------------|-------|
| `DATABRICKS_TOKEN` | `.env` (gitignored) | Databricks PAT — rotate every 90 days |
| `DATABRICKS_HOST` | `.env` | Your workspace URL |
| `AIRFLOW__CORE__FERNET_KEY` | `.env` | Generate once with `Fernet.generate_key()` |
| `AIRFLOW__WEBSERVER__SECRET_KEY` | `.env` | Generate with `secrets.token_hex(32)` |

## Generating a Databricks PAT

1. Databricks workspace → top-right avatar → **Settings**
2. **Developer** → **Access tokens** → **Generate new token**
3. Set a 90-day expiry
4. Copy immediately — it is shown only once

## What must NOT go into git

The `.gitignore` already covers these. Never force-add them:

```
.env
.env.*
*.dapi*
data/raw/
data/silver/
data/warehouse.duckdb
```

## Airflow connections

Store the Databricks PAT in Airflow via the UI or CLI — not in dags/. The DAG reads from
`databricks_default` connection, not from env vars directly.

```bash
airflow connections add databricks_default \
  --conn-type databricks \
  --conn-host "$DATABRICKS_HOST" \
  --conn-password "$DATABRICKS_TOKEN"
```

## dbt profiles

`dbt/profiles.yml` uses `{{ env_var('...') }}` — never put literal credentials there.
The file is committed but contains no secrets.
