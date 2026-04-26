from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "kacper",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}

SPARK_PACKAGES = (
    "io.delta:delta-spark_2.12:3.2.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
)


def _produce() -> None:
    from pathlib import Path

    from src.application.produce_listings import ProduceListings
    from src.infrastructure.kafka.producer import ListingProducer

    producer = ListingProducer(bootstrap_servers="kafka:29092")
    result = ProduceListings(producer).execute(Path("/opt/airflow/data/raw/vehicles.csv"))
    print(f"Kafka produce: {result}")
    producer.close()


with DAG(
    dag_id="car_price_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["portfolio", "de", "spark", "kafka"],
) as dag:
    kafka_produce = PythonOperator(
        task_id="kafka_produce",
        python_callable=_produce,
    )

    spark_bronze = SparkSubmitOperator(
        task_id="spark_bronze",
        application="/opt/airflow/src/application/stream_to_bronze.py",
        conn_id="spark_default",
        packages=SPARK_PACKAGES,
    )

    spark_silver = SparkSubmitOperator(
        task_id="spark_silver",
        application="/opt/airflow/src/application/transform_silver.py",
        conn_id="spark_default",
        packages=SPARK_PACKAGES,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir .",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir .",
    )

    kafka_produce >> spark_bronze >> spark_silver >> dbt_run >> dbt_test
