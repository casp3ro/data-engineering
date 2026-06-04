"""DAG import smoke tests — no Airflow runtime required."""
from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

DAG_MODULES = (
    "dags.car_price_pipeline",
    "dags.car_price_pipeline_local",
)


@pytest.fixture(autouse=True)
def _repo_on_path() -> None:
    root = Path(__file__).resolve().parents[3]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))


@pytest.mark.parametrize("module_name", DAG_MODULES)
def test_dag_module_imports(module_name: str) -> None:
    pytest.importorskip("airflow")
    mod = importlib.import_module(module_name)
    dag = getattr(mod, "dag", None)
    assert dag is not None, f"{module_name} must expose a module-level `dag`"
    assert dag.dag_id
