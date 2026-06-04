"""
Custom Databricks helpers for the car-price pipeline.
Role: supplements the stock DatabricksRunNowOperator with output parsing
      so that notebook metrics (row counts, filtered rows) surface in XCom.
"""
from __future__ import annotations

import json
import logging
from typing import Any

from airflow.models import BaseOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook

logger = logging.getLogger(__name__)


def get_databricks_run_output(run_id: int, conn_id: str = "databricks_default") -> dict[str, Any]:
    """
    Fetch the notebook exit output for each task in a Databricks run.
    Returns a dict keyed by task_key with the parsed JSON notebook exit value.
    """
    hook = DatabricksHook(databricks_conn_id=conn_id)
    run_output = hook.get_run_output(run_id)
    results: dict[str, Any] = {}
    for task in run_output.get("tasks", []):
        key = task.get("task_key", "unknown")
        notebook_output = task.get("notebook_output", {})
        result_str = notebook_output.get("result", "{}")
        try:
            results[key] = json.loads(result_str)
        except json.JSONDecodeError:
            results[key] = {"raw": result_str}
    return results


class DatabricksJobStatusSensor(BaseOperator):
    """
    Polls a Databricks job run until it reaches a terminal state.
    Pushes parsed notebook exit metrics to XCom under key 'notebook_outputs'.
    Use when you need to fan-out on metrics after the run completes.
    """

    template_fields = ("run_id",)

    def __init__(
        self,
        *,
        run_id: int | str,
        databricks_conn_id: str = "databricks_default",
        polling_interval_seconds: int = 30,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.run_id = run_id
        self.databricks_conn_id = databricks_conn_id
        self.polling_interval_seconds = polling_interval_seconds

    def execute(self, context: Any) -> dict[str, Any]:
        import time

        hook = DatabricksHook(databricks_conn_id=self.databricks_conn_id)
        run_id = int(self.run_id)
        while True:
            state = hook.get_run_state(run_id)
            logger.info("Databricks run %s state: %s", run_id, state)
            if state.is_terminal:
                if not state.is_successful:
                    raise RuntimeError(f"Databricks run {run_id} failed with state: {state}")
                break
            time.sleep(self.polling_interval_seconds)

        outputs = get_databricks_run_output(run_id, self.databricks_conn_id)
        context["ti"].xcom_push(key="notebook_outputs", value=outputs)
        logger.info("Notebook outputs: %s", outputs)
        return outputs
