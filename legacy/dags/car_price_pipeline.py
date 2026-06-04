"""
DAG: car_price_pipeline
Role: orchestrates the full Kafka → Databricks → dbt pipeline on a daily schedule.

Task order:
  produce_to_kafka → wait_for_kafka → databricks_pipeline → dbt_run → dbt_test → notify_success
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.sensors.time_delta import TimeDeltaSensor

logger = logging.getLogger(__name__)

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def _produce_to_kafka(**context: object) -> None:
    from pathlib import Path

    from kafka.config import KafkaConfig
    from kafka.producer import ListingProducer

    csv_path = Path(Variable.get("CAR_PRICE_CSV_PATH", default_var=os.getenv("CAR_PRICE_CSV_PATH", "")))
    config = KafkaConfig()
    producer = ListingProducer(config)
    result = producer.produce_from_csv(csv_path)
    producer.close()
    logger.info("Kafka produce complete: %s", result)
    context["ti"].xcom_push(key="produce_result", value={"success": result.success_count, "errors": result.error_count})


def _notify_success(**context: object) -> None:
    ti = context["ti"]
    produce = ti.xcom_pull(task_ids="produce_to_kafka", key="produce_result") or {}
    logger.info(
        "Pipeline complete — produced=%s errors=%s dag_run=%s",
        produce.get("success"),
        produce.get("errors"),
        context.get("run_id"),
    )


with DAG(
    dag_id="car_price_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    is_paused_upon_creation=True,
    tags=["car-price", "production", "databricks"],
    doc_md=__doc__,
) as dag:

    produce_to_kafka = PythonOperator(
        task_id="produce_to_kafka",
        python_callable=_produce_to_kafka,
    )

    # Give Kafka 30s to settle before triggering Databricks streaming ingest
    wait_for_kafka = TimeDeltaSensor(
        task_id="wait_for_kafka",
        delta=timedelta(seconds=30),
    )

    databricks_pipeline = DatabricksRunNowOperator(
        task_id="databricks_pipeline",
        databricks_conn_id="databricks_default",
        job_id="{{ var.value.DATABRICKS_JOB_ID }}",
        wait_for_termination=True,
        polling_period_seconds=30,
    )

    verify_gold_export = BashOperator(
        task_id="verify_gold_export",
        bash_command="python /opt/airflow/scripts/publish_gold_to_minio.py",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "PIPELINE_MODE=databricks DBT_TARGET=databricks "
            "dbt run --profiles-dir . --target databricks"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "PIPELINE_MODE=databricks DBT_TARGET=databricks "
            "dbt test --profiles-dir . --target databricks"
        ),
        trigger_rule="all_success",
    )

    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=_notify_success,
    )

    (
        produce_to_kafka
        >> wait_for_kafka
        >> databricks_pipeline
        >> verify_gold_export
        >> dbt_run
        >> dbt_test
        >> notify_success
    )
