from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.infrastructure.spark.delta_writer import DeltaWriter

BRONZE_PATH = "s3a://bronze/listings"


class TransformSilver:
    def __init__(self, spark: SparkSession, writer: DeltaWriter) -> None:
        self._spark = spark
        self._writer = writer

    def execute(self) -> dict:
        bronze = self._spark.read.format("delta").load(BRONZE_PATH)
        silver = self._clean(bronze)
        silver = self._filter(silver)
        self._writer.upsert_silver(silver, "listings")
        return {"silver_count": silver.count()}

    def _clean(self, df: DataFrame) -> DataFrame:
        return (
            df.withColumn("make", F.lower(F.trim(F.col("make"))))
            .withColumn("model", F.lower(F.trim(F.col("model"))))
            .withColumn("state", F.upper(F.trim(F.col("state"))))
            .withColumn("log_price", F.log1p(F.col("price")))
            .dropDuplicates(["id"])
        )

    def _filter(self, df: DataFrame) -> DataFrame:
        return df.filter(
            F.col("price").between(500, 200_000)
            & F.col("year").between(1990, 2024)
            & F.col("mileage").between(0, 500_000)
            & F.col("make").isNotNull()
            & (F.col("make") != "")
        )
