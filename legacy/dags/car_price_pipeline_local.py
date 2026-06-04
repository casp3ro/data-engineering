"""
DAG: car_price_pipeline_local
Local lakehouse path: Kafka → Spark streaming bronze → Spark silver → dbt (MinIO) → DuckDB.
Requires Docker profile `local` (MinIO + Spark) and vehicles.csv mounted in Airflow data/.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor

logger = logging.getLogger(__name__)

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}


def _produce_to_kafka(**context: object) -> None:
    from kafka.config import KafkaConfig
    from kafka.producer import ListingProducer

    csv_path = Path(
        Variable.get(
            "CAR_PRICE_CSV_PATH",
            default_var=os.getenv("CAR_PRICE_CSV_PATH", "/opt/airflow/data/raw/vehicles.csv"),
        )
    )
    producer = ListingProducer(KafkaConfig())
    result = producer.produce_from_csv(csv_path)
    producer.close()
    logger.info("Kafka produce: %s", result)
    context["ti"].xcom_push(  # type: ignore[index]
        key="produce_result",
        value={"success": result.success_count, "errors": result.error_count},
    )


def _spark_stream_bronze() -> None:
    from src.application.stream_to_bronze import StreamToBronze
    from src.infrastructure.spark.session import get_spark_session

    spark = get_spark_session("AirflowStreamBronze")
    try:
        StreamToBronze(spark).run()
    finally:
        spark.stop()


def _spark_silver() -> None:
    from src.application.transform_silver import TransformSilver
    from src.infrastructure.spark.delta_writer import DeltaWriter
    from src.infrastructure.spark.session import get_spark_session

    spark = get_spark_session("AirflowTransformSilver")
    try:
        result = TransformSilver(spark, DeltaWriter()).execute()
        logger.info("Silver result: %s", result)
    finally:
        spark.stop()


def _setup_minio() -> None:
    from src.infrastructure.storage.minio_client import MinioClient

    MinioClient(
        endpoint=os.getenv("MINIO_ENDPOINT", "minio:9000"),
    ).setup_buckets()


with DAG(
    dag_id="car_price_pipeline_local",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    tags=["car-price", "local", "minio"],
    doc_md=__doc__,
) as dag:

    setup_minio = PythonOperator(
        task_id="setup_minio",
        python_callable=_setup_minio,
    )

    produce_to_kafka = PythonOperator(
        task_id="produce_to_kafka",
        python_callable=_produce_to_kafka,
    )

    wait_for_kafka = TimeDeltaSensor(
        task_id="wait_for_kafka",
        delta=timedelta(seconds=30),
    )

    spark_stream_bronze = PythonOperator(
        task_id="spark_stream_bronze",
        python_callable=_spark_stream_bronze,
    )

    spark_silver = PythonOperator(
        task_id="spark_silver",
        python_callable=_spark_silver,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "PIPELINE_MODE=local DBT_TARGET=local "
            "dbt run --profiles-dir . --target local"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "PIPELINE_MODE=local DBT_TARGET=local "
            "dbt test --profiles-dir . --target local"
        ),
    )

    (
        setup_minio
        >> produce_to_kafka
        >> wait_for_kafka
        >> spark_stream_bronze
        >> spark_silver
        >> dbt_run
        >> dbt_test
    )
