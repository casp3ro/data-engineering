#!/bin/bash
set -e
cd "$(dirname "$0")/../dbt"
uv run dbt run --profiles-dir .
uv run dbt test --profiles-dir .
