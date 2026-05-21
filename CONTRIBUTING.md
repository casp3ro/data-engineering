# Contributing

## Branch naming

```
feat/<scope>-<short-description>
fix/<scope>-<short-description>
refactor/<scope>
```

## Commit convention

```
feat(kafka): add schema registry support
fix(dbt): correct max_year var to 2025
refactor(databricks): split gold notebook into separate tasks
chore(ci): add coverage gate
```

## PR checklist

Before opening a PR, verify:

- [ ] `uv run ruff check .` — zero errors
- [ ] `uv run mypy src/ kafka/` — zero errors
- [ ] `uv run pytest tests/unit/ -v` — all tests pass
- [ ] `cd dbt && uv run dbt parse` — zero errors
- [ ] `python -m py_compile` passes on all Databricks notebooks
- [ ] No hardcoded credentials (grep for `minioadmin`, `dapi`, `token=`)
- [ ] New Databricks notebook changes include `dbutils.notebook.exit()` with metrics

## Running tests

```bash
# Fast (no JVM, no Kafka)
uv run pytest tests/unit/ -v

# With Kafka running locally
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 uv run pytest tests/integration/ -m integration

# Spark tests (requires JVM)
uv run pytest tests/ -m spark -v
```
