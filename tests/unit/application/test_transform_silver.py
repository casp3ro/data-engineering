import math
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.application.transform_silver import TransformSilver


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_transform_silver")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    return session


@pytest.fixture
def ts(spark: SparkSession) -> TransformSilver:
    return TransformSilver(spark, MagicMock())


_SCHEMA = StructType([
    StructField("id",      StringType(),  True),
    StructField("make",    StringType(),  True),
    StructField("model",   StringType(),  True),
    StructField("year",    IntegerType(), True),
    StructField("price",   DoubleType(),  True),
    StructField("mileage", IntegerType(), True),
    StructField("state",   StringType(),  True),
])


def _df(spark: SparkSession, rows: list[tuple]) -> object:
    return spark.createDataFrame(rows, _SCHEMA)


@pytest.mark.spark
def test_clean_lowercases_make(ts: TransformSilver, spark: SparkSession) -> None:
    result = ts._clean(_df(spark, [("1", "Toyota", "Camry", 2018, 15000.0, 50000, "ca")]))
    assert result.first()["make"] == "toyota"


@pytest.mark.spark
def test_clean_uppercases_state(ts: TransformSilver, spark: SparkSession) -> None:
    result = ts._clean(_df(spark, [("1", "toyota", "camry", 2018, 15000.0, 50000, "ca")]))
    assert result.first()["state"] == "CA"


@pytest.mark.spark
def test_clean_adds_log_price(ts: TransformSilver, spark: SparkSession) -> None:
    result = ts._clean(_df(spark, [("1", "toyota", "camry", 2018, 15000.0, 50000, "ca")]))
    assert "log_price" in result.columns
    assert abs(result.first()["log_price"] - math.log1p(15000.0)) < 1e-6


@pytest.mark.spark
def test_clean_drops_duplicate_ids(ts: TransformSilver, spark: SparkSession) -> None:
    rows = [
        ("1", "toyota", "camry", 2018, 15000.0, 50000, "ca"),
        ("1", "toyota", "camry", 2018, 15000.0, 50000, "ca"),
    ]
    result = ts._clean(_df(spark, rows))
    assert result.count() == 1


@pytest.mark.spark
def test_filter_removes_price_below_minimum(ts: TransformSilver, spark: SparkSession) -> None:
    rows = [
        ("1", "toyota", "camry", 2018, 100.0,   50000, "ca"),  # too cheap
        ("2", "honda",  "civic", 2019, 15000.0, 30000, "ny"),  # valid
    ]
    result = ts._filter(_df(spark, rows))
    assert result.count() == 1
    assert result.first()["id"] == "2"


@pytest.mark.spark
def test_filter_removes_price_above_maximum(ts: TransformSilver, spark: SparkSession) -> None:
    rows = [
        ("1", "ford",  "f150",  2020, 250000.0, 10000, "tx"),  # too expensive
        ("2", "honda", "civic", 2019,  15000.0, 30000, "ny"),  # valid
    ]
    result = ts._filter(_df(spark, rows))
    assert result.count() == 1
    assert result.first()["id"] == "2"


@pytest.mark.spark
def test_filter_removes_year_before_minimum(ts: TransformSilver, spark: SparkSession) -> None:
    rows = [
        ("1", "toyota", "camry", 1970, 15000.0, 50000, "ca"),  # too old (MIN_YEAR=1980)
        ("2", "honda",  "civic", 2019, 15000.0, 30000, "ny"),  # valid
    ]
    result = ts._filter(_df(spark, rows))
    assert result.count() == 1
    assert result.first()["id"] == "2"


@pytest.mark.spark
def test_filter_excludes_empty_and_unknown_make(ts: TransformSilver, spark: SparkSession) -> None:
    rows = [
        ("1", "unknown", "x", 2018, 15000.0, 50000, "ca"),
        ("2", "",        "y", 2018, 15000.0, 50000, "ca"),
        ("3", "toyota",  "z", 2018, 15000.0, 50000, "ca"),
    ]
    result = ts._filter(_df(spark, rows))
    assert result.count() == 1
    assert result.first()["id"] == "3"
