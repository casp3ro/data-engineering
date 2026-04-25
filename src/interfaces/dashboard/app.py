import pandas as pd
import plotly.express as px
import streamlit as st
from pyspark.sql import SparkSession

st.set_page_config(page_title="Car Price Pipeline", page_icon="🚗", layout="wide")


@st.cache_resource
def get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("Dashboard")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )


@st.cache_data
def load(path: str) -> pd.DataFrame:
    return get_spark().read.format("delta").load(path).toPandas()


st.title("🚗 Car Price Pipeline")
st.caption("Spark · Delta Lake · Kafka · dbt · Airflow · MinIO")

summary = load("s3a://gold/mart_listings_summary")
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Listings", f"{summary['total_listings'][0]:,}")
c2.metric("Unique Makes", summary["unique_makes"][0])
c3.metric("States Covered", summary["states_covered"][0])
c4.metric("Median Price", f"${summary['overall_median_price'][0]:,}")

st.divider()

st.subheader("Median Price by Make — Top 20")
makes = load("s3a://gold/mart_price_by_make")
st.plotly_chart(
    px.bar(
        makes.head(20),
        x="make",
        y="median_price",
        color="listing_count",
        color_continuous_scale="Blues",
    ),
    use_container_width=True,
)

st.subheader("Price Depreciation by Year")
depr = load("s3a://gold/mart_price_by_year")
st.plotly_chart(
    px.line(depr, x="year", y="median_price", color="make"),
    use_container_width=True,
)

st.subheader("Median Price by State")
geo = load("s3a://gold/mart_price_by_state")
st.plotly_chart(
    px.choropleth(
        geo,
        locations="state",
        locationmode="USA-states",
        color="median_price",
        scope="usa",
        color_continuous_scale="Viridis",
    ),
    use_container_width=True,
)
