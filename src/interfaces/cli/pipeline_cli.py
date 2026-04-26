from __future__ import annotations

from pathlib import Path

import click

from src.application.produce_listings import ProduceListings
from src.application.stream_to_bronze import StreamToBronze
from src.application.transform_silver import TransformSilver
from src.infrastructure.kafka.producer import ListingProducer
from src.infrastructure.spark.delta_writer import DeltaWriter
from src.infrastructure.spark.session import get_spark_session


@click.group(help="Car Price Pipeline CLI")
def cli() -> None:
    pass


@cli.command("produce")
@click.option("--csv", "csv_path", type=click.Path(exists=True, dir_okay=False, path_type=Path), required=True)
@click.option("--bootstrap", default="localhost:9092", show_default=True)
def produce(csv_path: Path, bootstrap: str) -> None:
    producer = ListingProducer(bootstrap_servers=bootstrap)
    result = ProduceListings(producer).execute(csv_path)
    click.echo(result)
    producer.close()


@cli.command("bronze")
def bronze() -> None:
    spark = get_spark_session("StreamToBronze")
    StreamToBronze(spark).run()


@cli.command("silver")
def silver() -> None:
    spark = get_spark_session("TransformSilver")
    result = TransformSilver(spark, DeltaWriter()).execute()
    click.echo(result)


if __name__ == "__main__":
    cli()
