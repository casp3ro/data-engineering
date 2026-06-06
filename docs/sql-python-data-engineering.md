# Data Engineering — SQL i Python: koncepty, które musisz rozumieć

> Dokument do przeczytania i wracania. Każdy temat: **co to → dlaczego ważne w DE → konkretny przykład → pułapka**.
> Pojęcia po angielsku (bo tak działa branża), tłumaczenie po polsku.
> Przykłady oparte na danych z projektu **Lingua** (users, events, subscriptions).

---

# CZĘŚĆ I — SQL pod Data Engineering

SQL to **najważniejszy** język DE. Nie Python. 80% transformacji danych w nowoczesnym stacku (dbt) to czysty SQL. Jeśli opanujesz SQL na poziomie poniżej — jesteś silnym DE.

## 1. Myślenie zbiorowe (set-based), nie pętlowe

Najważniejsza zmiana w głowie. W Pythonie myślisz: „dla każdego wiersza zrób X". W SQL mówisz: „chcę taki **zbiór** wynikowy" — silnik sam zdecyduje *jak* go policzyć.

```sql
-- ŹLE (myślenie pętlowe, gdybyś mógł): "iteruj po userach, licz sesje"
-- DOBRZE (set-based): opisz wynik, silnik zoptymalizuje
SELECT user_id, count(*) AS events
FROM raw.events
GROUP BY user_id;
```

SQL jest **deklaratywny** — opisujesz *co* chcesz, nie *jak*. To dlatego ten sam SQL działa na DuckDB i na Snowflake nad petabajtami.

## 2. Logiczna kolejność wykonania (to rozwiązuje 50% Twoich błędów)

Piszesz `SELECT` na górze, ale silnik **logicznie** wykonuje zapytanie w innej kolejności:

```
1. FROM / JOIN      ← skąd dane
2. WHERE            ← filtr wierszy (przed grupowaniem)
3. GROUP BY         ← grupowanie
4. HAVING           ← filtr grup (po agregacji)
5. SELECT           ← wybór kolumn + aliasy
6. DISTINCT
7. ORDER BY
8. LIMIT
```

**Konsekwencje, które gryzą każdego:**

```sql
-- BŁĄD: aliasu z SELECT nie widać w WHERE (SELECT wykonuje się PÓŹNIEJ)
SELECT user_id, count(*) AS n
FROM raw.events
WHERE n > 5            -- ❌ "n" jeszcze nie istnieje
GROUP BY user_id;

-- DOBRZE: filtr agregatu należy do HAVING
SELECT user_id, count(*) AS n
FROM raw.events
GROUP BY user_id
HAVING count(*) > 5;   -- ✅
```

Zasada: **WHERE filtruje wiersze (przed GROUP BY), HAVING filtruje grupy (po).**

## 3. GROUP BY i agregacje — pułapki

```sql
-- Każda kolumna w SELECT musi być albo w GROUP BY, albo w agregacie
SELECT
  learning_language,
  count(*)               AS users,
  count(DISTINCT country) AS countries
FROM raw.users
GROUP BY learning_language;   -- learning_language MUSI tu być
```

Różnica, którą trzeba czuć:

| Wyrażenie | Liczy |
|---|---|
| `count(*)` | wszystkie wiersze (też z NULL-ami) |
| `count(col)` | wiersze gdzie `col IS NOT NULL` |
| `count(DISTINCT col)` | unikalne nie-NULL wartości |

**Pułapka:** `avg(col)` ignoruje NULL-e. Jeśli chcesz traktować NULL jak 0 — najpierw `coalesce(col, 0)`.

## 4. JOIN — i najgroźniejszy błąd w DE: fan-out

```sql
INNER JOIN   -- tylko pasujące z obu stron
LEFT JOIN    -- wszystkie z lewej + pasujące z prawej (reszta NULL)
FULL JOIN    -- wszystko z obu, niepasujące jako NULL
CROSS JOIN   -- iloczyn kartezjański (uwaga!)
```

**Fan-out (rozmnożenie wierszy)** — klasyk, który zawyża sumy. Jeśli joinujesz `users` (1 wiersz/user) z `subscriptions` (user może mieć kilka) i potem sumujesz MRR — policzysz usera tyle razy, ile ma subskrypcji:

```sql
-- ❌ PUŁAPKA: jeśli user ma 2 subskrypcje, jego dane się zdublują
SELECT u.user_id, count(e.event_id) AS events, s.mrr_usd
FROM raw.users u
LEFT JOIN raw.events e ON e.user_id = u.user_id        -- fan-out!
LEFT JOIN raw.subscriptions s ON s.user_id = u.user_id -- kolejny fan-out!
GROUP BY u.user_id, s.mrr_usd;
-- events policzy się błędnie (zwielokrotnione przez subskrypcje)

-- ✅ DOBRZE: agreguj PRZED joinem (pre-aggregate), albo joinuj 1:1
WITH ev AS (
  SELECT user_id, count(*) AS events FROM raw.events GROUP BY user_id
)
SELECT u.user_id, ev.events
FROM raw.users u
LEFT JOIN ev ON ev.user_id = u.user_id;
```

**Druga pułapka:** filtr na prawej tabeli w `WHERE` zamienia LEFT JOIN w INNER:

```sql
-- to NIE jest już LEFT JOIN (gubisz userów bez eventów):
LEFT JOIN raw.events e ON e.user_id = u.user_id
WHERE e.platform = 'ios'        -- ❌ wywala NULL-e = de facto INNER
-- popraw: warunek przenieś do ON
LEFT JOIN raw.events e ON e.user_id = u.user_id AND e.platform = 'ios'  -- ✅
```

## 5. CTE (`WITH`) — czytelność jest funkcją produkcyjną

CTE = nazwany podzbiór. Buduj zapytania **drabinkowo**, krok po kroku. To jak czytelne funkcje w kodzie.

```sql
WITH daily AS (                      -- krok 1: zdarzenia per user per dzień
  SELECT user_id, date_trunc('day', event_ts::timestamp) AS d
  FROM raw.events
  GROUP BY 1, 2
),
counts AS (                          -- krok 2: ile aktywnych dni per user
  SELECT user_id, count(*) AS active_days
  FROM daily GROUP BY 1
)
SELECT
  active_days,
  count(*) AS users                  -- krok 3: rozkład
FROM counts GROUP BY 1 ORDER BY 1;
```

Senior pisze 5 czytelnych CTE zamiast jednego zagnieżdżonego potwora. **Czytelny SQL > sprytny SQL.**

## 6. Window functions — serce zaawansowanego SQL

To temat, który najczęściej oddziela „umiem SELECT" od „umiem DE". Window function liczy **po oknie wierszy**, ale — w przeciwieństwie do `GROUP BY` — **nie zwija** wierszy. Każdy wiersz zostaje, dostaje dodatkową kolumnę.

Anatomia:
```sql
funkcja() OVER (
  PARTITION BY ...   -- podział na grupy (opcjonalny)
  ORDER BY ...       -- porządek w grupie (dla rankingów / bieżących sum)
  ROWS/RANGE ...     -- ramka (frame), które wiersze liczyć
)
```

**a) Ranking i deduplikacja — `ROW_NUMBER()`**
```sql
-- pierwszy event każdego usera (np. data aktywacji)
SELECT *
FROM (
  SELECT *,
    row_number() OVER (PARTITION BY user_id ORDER BY event_ts) AS rn
  FROM raw.events
) WHERE rn = 1;
```
To **najważniejszy** wzorzec dedup: `ROW_NUMBER` w partycji + `WHERE rn = 1`. Używasz go, by usunąć duplikaty (pamiętasz „bałagan" w danych Lingua — 0.3% zdublowanych eventów? Tak się je usuwa).

`RANK()` vs `DENSE_RANK()` vs `ROW_NUMBER()`:
| Funkcja | Przy remisie |
|---|---|
| `ROW_NUMBER` | 1,2,3,4 (zawsze unikalne) |
| `RANK` | 1,1,3,4 (dziury) |
| `DENSE_RANK` | 1,1,2,3 (bez dziur) |

**b) Poprzedni/następny wiersz — `LAG` / `LEAD`**
```sql
-- ile dni minęło od poprzedniego logowania (do liczenia sesji / retencji)
SELECT user_id, event_ts,
  lag(event_ts) OVER (PARTITION BY user_id ORDER BY event_ts) AS prev_ts
FROM raw.events
WHERE event_type = 'app_opened';
```

**c) Bieżąca suma / średnia krocząca — `SUM() OVER`**
```sql
-- skumulowana liczba rejestracji w czasie
SELECT d,
  count(*)                              AS signups,
  sum(count(*)) OVER (ORDER BY d)       AS cumulative
FROM (SELECT date_trunc('day', signup_ts::timestamp) AS d FROM raw.users)
GROUP BY d ORDER BY d;
```

**d) Ramka (frame) — subtelna pułapka:**
```sql
-- średnia krocząca z 7 dni
avg(x) OVER (ORDER BY d ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
```
Domyślna ramka przy `ORDER BY` to `RANGE UNBOUNDED PRECEDING AND CURRENT ROW`. **Pułapka:** `RANGE` przy zduplikowanych kluczach porządku liczy wszystkie remisy razem — często chcesz jawnie `ROWS`.

## 7. Czas, kohorty, retention — codzienność DE

```sql
date_trunc('day',   ts)   -- ścięcie do dnia/tygodnia/miesiąca
date_diff('day', a, b)    -- różnica
extract(dow FROM ts)      -- dzień tygodnia
```

**Date spine** (oś czasu) — by mieć też dni z zerem (inaczej braki w wykresach):
```sql
-- DuckDB: generate_series tworzy ciągłą oś dat
SELECT gs::date AS day
FROM generate_series(DATE '2025-06-01', DATE '2025-11-30', INTERVAL 1 DAY) AS t(gs);
-- potem LEFT JOIN aktywność -> dni bez eventów dostaną 0, nie znikną
```

**Retention (uproszczony D-N):** dla każdego usera liczysz dni aktywne względem dnia rejestracji, potem agregujesz kohortami. To klasyczne zadanie rekrutacyjne — robi się je przez `date_diff` + `GROUP BY` kohorta + window/agregaty.

## 8. NULL — logika trójwartościowa (three-valued logic)

NULL to nie 0 i nie „". To „nieznane". `NULL = NULL` daje… `NULL` (nie TRUE!).

```sql
WHERE col = NULL        -- ❌ nigdy nie zadziała
WHERE col IS NULL       -- ✅

coalesce(col, 0)        -- pierwszy nie-NULL
nullif(a, b)            -- NULL jeśli a = b (np. dzielenie przez 0)
```

**Najgroźniejsza pułapka — `NOT IN` z NULL-em w zbiorze:**
```sql
-- jeśli podzapytanie zwróci choć jeden NULL, CAŁOŚĆ zwraca 0 wierszy
WHERE user_id NOT IN (SELECT user_id FROM subscriptions)  -- ❌ ryzyko
-- bezpiecznie:
WHERE NOT EXISTS (SELECT 1 FROM subscriptions s WHERE s.user_id = u.user_id)  -- ✅
```

## 9. Wzorce DE, które robisz w kółko

| Wzorzec | Narzędzie |
|---|---|
| **Deduplikacja** | `ROW_NUMBER() PARTITION BY klucz ORDER BY ts` → `WHERE rn=1` |
| **Sesjonizacja** (grupowanie eventów w sesje) | `LAG` + flaga „nowa sesja gdy przerwa > 30 min" + `SUM() OVER` jako session_id |
| **Gap-and-island** (ciągłe okresy, np. streaki) | `ROW_NUMBER` minus data → grupa |
| **Running total / moving average** | `SUM/AVG OVER (ORDER BY ...)` |
| **Pivot** (wiersze→kolumny) | `CASE WHEN` + agregacja lub `PIVOT` (DuckDB) |
| **SCD** (historia zmian wymiaru) | `valid_from`/`valid_to`, dbt snapshots |

Sesjonizacja na danych Lingua to **dokładnie** Twój model `fct_sessions` w M3.

## 10. Wydajność SQL — co naprawdę liczy się w hurtowni

Hurtownie są **kolumnowe (columnar)** — czytają tylko potrzebne kolumny.

1. **Nie `SELECT *`** w martach — czytasz tylko potrzebne kolumny (mniej I/O).
2. **Filtruj wcześnie (predicate pushdown)** — `WHERE` jak najbliżej źródła, najlepiej na kolumnie partycjonującej (np. data).
3. **Partycjonowanie / clustering** — dane podzielone po dacie → silnik pomija nieczytane bloki (*partition pruning*).
4. **Agreguj późno, joinuj na zagregowanym** — unikasz fan-outu i przerzucania ton danych.
5. **`EXPLAIN`** / `EXPLAIN ANALYZE` — czytaj plan zapytania; szukaj pełnych skanów i eksplodujących joinów.
6. **Materializacja** — drogie modele licz raz (`table`/`incremental`), nie przy każdym zapytaniu (`view`).

---

# CZĘŚĆ II — Python pod Data Engineering

## 1. Jaka jest rola Pythona w DE

Python to **klej i orkiestracja**, nie maszyna obliczeniowa. Używasz go do:
- **Ingestion / EL** — pobranie danych (API, pliki, bazy) i załadowanie do hurtowni.
- **Orkiestracja** — Airflow/Dagster piszesz w Pythonie.
- **Walidacja, testy, automatyzacja, „lekka" logika.**

Ciężką transformację oddajesz **hurtowni (SQL/dbt)** albo silnikom kolumnowym (Polars, Spark). Python ma być cienką warstwą.

## 2. Wektoryzacja — nie iteruj po wierszach

Najczęstszy błąd początkującego DE: pętla po wierszach DataFrame.

```python
# ❌ WOLNE (iterrows ~ setki razy wolniej)
for i, row in df.iterrows():
    df.loc[i, "x2"] = row["x"] * 2

# ✅ SZYBKIE (wektoryzacja — operacja na całej kolumnie naraz)
df["x2"] = df["x"] * 2
```
Powód: pandas/polars operują na **kolumnach** w skompilowanym C/Rust. Pętla w Pythonie zabija tę przewagę. Zasada: **jeśli piszesz `for` po wierszach danych — prawie zawsze robisz to źle.**

## 3. pandas — fundament i jego pułapki

```python
import pandas as pd
df = pd.read_csv("data/raw/events.csv")
df.head(); df.info(); df.describe()           # zawsze poznaj dane
df["event_type"].value_counts()
df.groupby("user_id").size()
df.merge(users, on="user_id", how="left")     # to jest JOIN
```

**Pułapki, które gryzą (część znasz z fast.ai):**

- **`SettingWithCopyWarning` / `ChainedAssignmentError` (pandas 2.x).** Wynika z modyfikacji „widoku" zamiast kopii.
  ```python
  # ❌ łańcuch [ ][ ] = niejednoznaczne
  df[df.x > 0]["y"] = 1
  # ✅ jednoznaczny .loc
  df.loc[df.x > 0, "y"] = 1
  ```
  (Tymczasowy obejście `pd.options.mode.copy_on_write = False` maskuje problem — lepiej pisać poprawnie przez `.loc`.)

- **Typy (dtypes).** CSV nie ma schematu → pandas zgaduje. Kolumny tekstowe lądują jako `object` (drogie w pamięci). Parsuj daty i kategorie jawnie:
  ```python
  pd.read_csv("events.csv", parse_dates=["event_ts"],
              dtype={"platform": "category"})
  ```

- **Pamięć.** pandas trzyma WSZYSTKO w RAM. Duży plik → `chunksize` albo Polars/DuckDB.
  ```python
  for chunk in pd.read_csv("huge.csv", chunksize=100_000):
      process(chunk)
  ```

## 4. Polars — nowoczesny standard

Polars (Rust + Apache Arrow) jest często 5–30× szybszy od pandas i ma **lazy execution** — buduje plan i optymalizuje, jak SQL.

```python
import polars as pl

# lazy: nic się nie liczy aż do .collect()
out = (
    pl.scan_csv("data/raw/events.csv")          # scan = lazy
      .filter(pl.col("event_type") == "lesson_completed")
      .group_by("user_id")
      .agg(pl.len().alias("lessons"))
      .sort("lessons", descending=True)
      .collect()                                 # tu wykonuje plan
)
```
Dlaczego lazy jest ważne: optymalizator wykona **predicate pushdown** i **projection pushdown** (czyta tylko potrzebne kolumny/wiersze) — to ta sama idea co w hurtowni. Dla zbiorów do kilku/kilkunastu GB Polars na jednej maszynie bije Sparka prostotą.

## 5. Formaty plików — Parquet to domyślny wybór DE

| Format | Charakter | Kiedy |
|---|---|---|
| **CSV** | tekstowy, wierszowy, **bez typów/schematu** | wymiana, wejście od ludzi |
| **JSON / JSONL** | zagnieżdżony, gadatliwy | API, logi |
| **Parquet** | **binarny, kolumnowy, kompresja, schemat w pliku** | standard analityki/DE |

Parquet > CSV w DE prawie zawsze: kolumnowy (czytasz tylko potrzebne kolumny), skompresowany (mniej I/O), ma typy (brak zgadywania, brak `object`).

```python
df.to_parquet("events.parquet")                 # zapis
pl.read_parquet("events.parquet")               # odczyt
# DuckDB czyta Parquet bez ładowania:
# SELECT * FROM 'events.parquet' WHERE event_type = 'signup';
```

## 6. Praca z bazami i hurtownią z Pythona

```python
import duckdb
con = duckdb.connect("warehouse/lingua.duckdb")
df = con.execute("SELECT * FROM raw.events LIMIT 5").df()   # wynik jako DataFrame
con.register("tmp", df)                                     # DataFrame jako tabela SQL
```

Zasady przy „prawdziwych" bazach (Postgres itd.):
- **Batch / chunk** zapisów (`executemany`, `COPY`), nie wiersz po wierszu.
- **Connection pooling** przy wielu zapytaniach.
- **Parametryzuj** zapytania (`?` / `%s`) — nigdy f-string z danymi (SQL injection, błędy typów).

## 7. Idempotencja w ingestion — fundament niezawodności

Pipeline musi dać ten sam wynik po wielokrotnym uruchomieniu (np. po retry). Wzorce:

```python
# A) full refresh: nadpisz całość (proste, OK dla małych)
con.execute("CREATE OR REPLACE TABLE raw.users AS SELECT * FROM read_csv_auto('users.csv')")

# B) upsert / merge: wstaw nowe, zaktualizuj istniejące (dla przyrostowych)
#   MERGE INTO target USING source ON target.id = source.id
#     WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED THEN INSERT ...

# C) dedup po załadowaniu: ROW_NUMBER() ... WHERE rn = 1
```
To dokładnie zasada z `scripts/load_raw.py` w Twoim projekcie (`CREATE OR REPLACE`).

## 8. Walidacja danych — ufaj, ale sprawdzaj

„Garbage in, garbage out". Sprawdzaj kontrakt danych zanim wpuścisz je dalej.

```python
from pydantic import BaseModel
class Event(BaseModel):           # walidacja schematu per rekord
    event_id: int
    user_id: int
    event_type: str
    event_ts: str

# Lekkie asercje pipeline'owe:
assert df["user_id"].notna().all(),            "user_id nie może być NULL"
assert df["event_id"].is_unique,               "event_id musi być unikalny"
assert df["event_type"].isin(VALID).all(),     "nieznany event_type"
```
W produkcji testy danych żyją w **dbt** (`unique`, `not_null`, `relationships`, `accepted_values`) lub w narzędziach typu Great Expectations / dbt tests. Idea: **testuj dane, nie tylko kod.**

## 9. Środowisko i struktura projektu

```bash
uv venv --python 3.11           # izolacja zależności (masz z fast.ai)
source .venv/bin/activate
uv pip install -r requirements.txt
```
- **Przypnij wersje** (`requirements.txt` / `pyproject.toml`) — powtarzalność.
- **Nie commituj danych ani sekretów** (`.gitignore`, zmienne środowiskowe / `.env`).
- **Struktura:** `scripts/` (EL), `dbt/` (transformacje), `data/` (wejście), `warehouse/` (DuckDB, ignorowane).

## 10. Best practices kodu DE

1. **Idempotencja** — re-run bez efektów ubocznych (upsert/replace, nie ślepy insert).
2. **Konfiguracja poza kodem** — ścieżki, klucze w env/config, nie zaszyte.
3. **Logowanie zamiast `print`** — `logging`, poziomy INFO/ERROR, by widzieć co padło o 3 w nocy.
4. **Retry z backoffem** — sieć/API zawodzą; ponawiaj z rosnącym opóźnieniem.
5. **Małe, czyste funkcje** — jedna robi jedno; łatwiej testować.
6. **Testy** — jednostkowe dla logiki transformacji + testy danych dla wyników.
7. **Wersjonuj wszystko w Git** — SQL i Python to kod; PR + review.

---

# CZĘŚĆ III — Koncepty przekrojowe (spinają SQL i Python)

## ETL vs ELT
- **ETL:** transformacja *przed* załadowaniem (stary świat, osobny serwer transformacji).
- **ELT:** ładujesz surowe, transformujesz *w hurtowni* SQL-em/dbt. **To dzisiejszy standard** — bo hurtownie są tanie i mocne, a surowiec zostaje (można przeliczyć).

## OLTP vs OLAP
- **OLTP** (Postgres, app DB) — wierszowe, wiele małych zapisów, aplikacja.
- **OLAP** (Snowflake, BigQuery, DuckDB) — kolumnowe, ciężkie odczyty/agregaty, analityka.
DE łączy oba: ściąga z OLTP → ładuje do OLAP → modeluje.

## Warstwy / medallion (bronze→silver→gold)
- **raw / bronze** — surowe, nietykalne (audyt, możliwość przeliczenia).
- **staging / silver** — oczyszczone, typy, nazwy, dedup (1:1 ze źródłem).
- **marts / gold** — modele biznesowe: `fct_*`, `dim_*` (gotowe dla BI).
Po co: **separacja odpowiedzialności** — błąd łapiesz w warstwie, nie w 800-linijkowym zapytaniu.

## Modelowanie wymiarowe (dimensional modeling)
- **Fact (fakt)** — zdarzenia/metryki, dużo wierszy: `fct_sessions`, `fct_subscriptions`.
- **Dimension (wymiar)** — kontekst, mało wierszy: `dim_users`, `dim_date`.
- **Star schema** — fakty w środku, wymiary dokoła (proste, szybkie). Snowflake = wymiary znormalizowane (rzadziej).
- **Grain (ziarno)** — co reprezentuje jeden wiersz. **Pierwsza i najważniejsza decyzja** każdego modelu. „Jeden wiersz = jedna sesja jednego usera."
- **SCD (Slowly Changing Dimension)** — jak śledzić zmiany wymiaru w czasie (user zmienił plan): Type 1 = nadpisz, Type 2 = historia z `valid_from/valid_to`.

## Materializacja (w dbt)
| Typ | Co robi | Kiedy |
|---|---|---|
| `view` | zapytanie na żądanie | lekkie staging, zawsze świeże |
| `table` | przelicz i zapisz całość | marty często odpytywane |
| `incremental` | dolicz tylko nowe wiersze | duże fakty, kosztowny full-refresh |
| `ephemeral` | wklejone jako CTE | drobna logika współdzielona |

## Orkiestracja
- **DAG** — graf zależności zadań (acykliczny). dbt i Airflow myślą w DAG-ach.
- Daje: **scheduling** (harmonogram), **dependencies** (kolejność), **retry**, **idempotency**, **alerting**.
- Narzędzia: **Airflow** (standard rynkowy), **Dagster** (nowoczesny, asset-oriented).

## Data quality i observability
- **Testy** (dbt) — unique, not_null, relationships, accepted_values.
- **Freshness / SLA** — dane świeże do X, gotowe do godziny Y.
- **Lineage** — skąd pochodzi każda kolumna (dbt rysuje graf).
- **Observability** — monitoring objętości, opóźnień, anomalii (czy nagle 0 wierszy?).

---

## Jedna strona „ściągi"

**SQL — opanuj na pamięć:** logiczna kolejność wykonania · window functions (`ROW_NUMBER`/`LAG`/`SUM OVER`) · dedup przez `ROW_NUMBER ... WHERE rn=1` · fan-out i pre-agregacja · NULL i three-valued logic · CTE drabinkowe · `date_trunc` + date spine.

**Python — opanuj na pamięć:** wektoryzacja zamiast pętli · pandas pułapki (`.loc`, dtypes, pamięć) · Polars lazy · Parquet > CSV · idempotentny load (`CREATE OR REPLACE`/merge) · walidacja danych · logging + retry.

**Mentalność:** ELT (transformuj w hurtowni) · warstwy raw→staging→marts · jeden grain na model · testuj dane, nie tylko kod · wszystko w Git.
