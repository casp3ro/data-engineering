"""
Ładuje surowe CSV do DuckDB, do schematu `raw`.
Idempotentny: CREATE OR REPLACE = mozesz odpalac wielokrotnie bez duplikatow tabel.
Uruchom z katalogu glownego projektu:  python scripts/load_raw.py
"""
import duckdb, os

DB = "warehouse/lingua.duckdb"
os.makedirs("warehouse", exist_ok=True)

con = duckdb.connect(DB)
con.execute("CREATE SCHEMA IF NOT EXISTS raw;")

tables = {
    "users": "data/raw/users.csv",
    "events": "data/raw/events.csv",
    "subscriptions": "data/raw/subscriptions.csv",
}

for name, path in tables.items():
    con.execute(f"""
        CREATE OR REPLACE TABLE raw.{name} AS
        SELECT * FROM read_csv_auto('{path}', header=true);
    """)
    n = con.execute(f"SELECT count(*) FROM raw.{name}").fetchone()[0]
    print(f"  raw.{name:14s} -> {n:>7,} wierszy")

print("\nGotowe. Hurtownia:", DB)
con.close()
