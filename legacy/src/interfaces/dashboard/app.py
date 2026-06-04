import os
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="Car Price Pipeline", page_icon="🚗", layout="wide")

DB_PATH = os.getenv("WAREHOUSE_PATH", "data/warehouse.duckdb")

_ALLOWED_TABLES = frozenset({
    "mart_listings_summary",
    "mart_price_by_make",
    "mart_price_by_state",
    "mart_price_by_year",
})


@st.cache_data
def load(table: str) -> pd.DataFrame:
    if table not in _ALLOWED_TABLES:
        raise ValueError(f"Unknown table: {table!r}")
    conn = duckdb.connect(DB_PATH, read_only=True)
    df = conn.execute(f"SELECT * FROM {table}").df()  # noqa: S608 — table is whitelisted above
    conn.close()
    return df


st.title("🚗 Car Price Pipeline")
st.caption("Kafka · Spark · Delta Lake · dbt · DuckDB")

if not Path(DB_PATH).exists():
    st.warning(
        f"No warehouse at `{DB_PATH}`. "
        "Run `uv run python scripts/run_all.py` (local) or trigger the Airflow DAG after dbt completes."
    )
    st.stop()

try:
    summary = load("mart_listings_summary")
except duckdb.CatalogException:
    st.error(
        "Warehouse exists but mart tables are missing. Run the pipeline through dbt "
        "(`scripts/run_dbt.py` or Airflow `dbt_run`)."
    )
    st.stop()

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Listings", f"{summary['total_listings'][0]:,}")
c2.metric("Unique Makes", summary["unique_makes"][0])
c3.metric("States Covered", summary["states_covered"][0])
c4.metric("Median Price", f"${summary['overall_median_price'][0]:,}")

st.divider()

st.subheader("Median Price by Make — Top 20")
makes = load("mart_price_by_make")
st.plotly_chart(
    px.bar(
        makes.head(20),
        x="make",
        y="median_price",
        color="listing_count",
        color_continuous_scale="Plasma",
        labels={"median_price": "Median Price ($)", "make": "Make"},
    ),
    use_container_width=True,
)

st.subheader("Price Depreciation by Year")
depr = load("mart_price_by_year")
if "make" in depr.columns and depr["make"].nunique() > 1:
    chart = px.line(
        depr,
        x="year",
        y="median_price",
        color="make",
        labels={"median_price": "Median Price ($)", "year": "Year"},
    )
else:
    chart = px.line(
        depr,
        x="year",
        y="median_price",
        labels={"median_price": "Median Price ($)", "year": "Year"},
    )
st.plotly_chart(chart, use_container_width=True)

st.subheader("Median Price by State")
geo = load("mart_price_by_state")
st.plotly_chart(
    px.choropleth(
        geo,
        locations="state",
        locationmode="USA-states",
        color="median_price",
        scope="usa",
        color_continuous_scale="Plasma",
        labels={"median_price": "Median Price ($)"},
    ),
    use_container_width=True,
)
