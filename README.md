# Lingua Analytics — projekt kursu Data Engineering

Pipeline analityki produktowej dla fikcyjnej apki do nauki jezykow.
Surowe zdarzenia → czyste modele → metryki (DAU, retention, lejek, MRR, churn).

```
data/raw/        surowe CSV (users, events, subscriptions)
scripts/         load_raw.py  (CSV → DuckDB, warstwa raw)
dbt/             projekt dbt: staging → intermediate → marts
warehouse/       hurtownia DuckDB (generowana lokalnie, w .gitignore)
```

---

## M0 — Setup (zrob to teraz)

Wymagane: Python 3.11 + `uv` (masz z fast.ai).

```bash
# 1. srodowisko
cd lingua-analytics
uv venv --python 3.11
source .venv/bin/activate
uv pip install -r requirements.txt

# 2. zaladuj surowe dane do DuckDB
python scripts/load_raw.py
#  -> raw.users 4 000 | raw.events 260 509 | raw.subscriptions 406

# 3. sprawdz dbt
cd dbt
dbt debug          # ma byc "All checks passed!"
cd ..
```

### Pierwsze zapytanie (Twoj pierwszy „win")

Wejdz do hurtowni i policz zdarzenia wg typu:

```bash
duckdb warehouse/lingua.duckdb
```
```sql
SELECT event_type, count(*) AS n
FROM raw.events
GROUP BY event_type
ORDER BY n DESC;
-- .quit  zeby wyjsc
```

### Zadanie na rozgrzewke (przynies wynik na nastepna sesje)
1. Ilu jest unikalnych uzytkownikow w `raw.events`? (`COUNT(DISTINCT ...)`)
2. Ile rejestracji dziennie? (`DATE_TRUNC('day', ...)` + `GROUP BY`)
3. Zauwaz „bałagan": `SELECT DISTINCT country FROM raw.users;` — co jest nie tak?

To wszystko ogarniemy w **M1 (SQL)**. Daj znac jak masz `dbt debug` na zielono.
