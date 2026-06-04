#!/usr/bin/env python3
"""Fail fast when required .env variables for Docker/Airflow are missing."""
from __future__ import annotations

import sys
from pathlib import Path

REQUIRED = (
    "AIRFLOW__CORE__FERNET_KEY",
    "AIRFLOW__WEBSERVER__SECRET_KEY",
)

OPTIONAL_DATABRICKS = (
    "DATABRICKS_HOST",
    "DATABRICKS_TOKEN",
    "DATABRICKS_JOB_ID",
)


def _load_dotenv(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        values[key.strip()] = val.strip().strip('"').strip("'")
    return values


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    env_path = root / ".env"
    env = _load_dotenv(env_path)

    missing = [k for k in REQUIRED if not env.get(k)]
    if missing:
        print(f"Missing required keys in {env_path}:", ", ".join(missing))
        print("Copy infra/.env.example to .env and generate Fernet/secret keys.")
        sys.exit(1)

    if not all(env.get(k) for k in OPTIONAL_DATABRICKS):
        print("Note: Databricks vars unset — car_price_pipeline DAG will not run end-to-end.")

    print(f"Environment OK ({env_path})")


if __name__ == "__main__":
    main()
