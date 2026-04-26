import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="Car Price Pipeline", page_icon="🚗", layout="wide")

DB_PATH = "data/warehouse.duckdb"


@st.cache_data
def load(table: str) -> pd.DataFrame:
    conn = duckdb.connect(DB_PATH, read_only=True)
    df = conn.execute(f"SELECT * FROM {table}").df()
    conn.close()
    return df


st.title("🚗 Car Price Pipeline")
st.caption("Spark · Delta Lake · Kafka · dbt · DuckDB · MinIO")

summary = load("mart_listings_summary")
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Listings",  f"{summary['total_listings'][0]:,}")
c2.metric("Unique Makes",    summary['unique_makes'][0])
c3.metric("States Covered",  summary['states_covered'][0])
c4.metric("Median Price",   f"${summary['overall_median_price'][0]:,}")

st.divider()

st.subheader("Median Price by Make — Top 20")
makes = load("mart_price_by_make")
st.plotly_chart(
    px.bar(makes.head(20), x="make", y="median_price",
           color="listing_count", color_continuous_scale="RdYlGn",
           labels={"median_price": "Median Price ($)", "make": "Make"}),
    use_container_width=True,
)

st.subheader("Price Depreciation by Year")
depr = load("mart_price_by_year")
st.plotly_chart(
    px.line(depr, x="year", y="median_price", color="make",
            labels={"median_price": "Median Price ($)", "year": "Year"}),
    use_container_width=True,
)

st.subheader("Median Price by State")
geo = load("mart_price_by_state")
st.plotly_chart(
    px.choropleth(geo, locations="state", locationmode="USA-states",
                  color="median_price", scope="usa",
                  color_continuous_scale="RdYlGn",
                  labels={"median_price": "Median Price ($)"}),
    use_container_width=True,
)