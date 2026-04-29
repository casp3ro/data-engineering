from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "kacper",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}


def _produce() -> None:
    from pathlib import Path

    from src.application.produce_listings import ProduceListings
    from src.infrastructure.kafka.producer import ListingProducer

    producer = ListingProducer(bootstrap_servers="kafka:29092")
    result = ProduceListings(producer).execute(Path("/opt/airflow/data/raw/vehicles.csv"))
    print(f"Kafka produce: {result}")
    producer.close()


def _spark_bronze() -> None:
    from src.application.stream_to_bronze import StreamToBronze
    from src.infrastructure.spark.session import get_spark_session

    spark = get_spark_session(app_name="car_price_pipeline_bronze")
    try:
        StreamToBronze(spark).run()
    finally:
        spark.stop()


def _spark_silver() -> None:
    from src.application.transform_silver import TransformSilver
    from src.infrastructure.spark.delta_writer import DeltaWriter
    from src.infrastructure.spark.session import get_spark_session

    spark = get_spark_session(app_name="car_price_pipeline_silver")
    try:
        result = TransformSilver(spark, DeltaWriter()).execute()
        print(f"Transform silver: {result}")
    finally:
        spark.stop()


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

    spark_bronze = PythonOperator(
        task_id="spark_bronze",
        python_callable=_spark_bronze,
    )

    spark_silver = PythonOperator(
        task_id="spark_silver",
        python_callable=_spark_silver,
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
