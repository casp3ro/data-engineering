# Data Engineering — Kluczowe Pojęcia, Stack i Praktyki

---

## 1. Czym jest Data Engineering

Data Engineering to dyscyplina budowania infrastruktury i pipeline'ów które zbierają, przechowują, transformują i udostępniają dane — tak żeby data scientists, analitycy i aplikacje mogły z nich korzystać.

DE jest fundamentem każdego systemu AI/ML. Bez poprawnie zbudowanego pipeline'u nie ma danych do trenowania modeli.

**DE vs Data Science vs Analytics:**

| Rola           | Pytanie              | Narzędzia                  |
| -------------- | -------------------- | -------------------------- |
| Data Engineer  | Jak dostarczyć dane? | Spark, Kafka, Airflow, dbt |
| Data Scientist | Co z danych wynika?  | Python, PyTorch, sklearn   |
| Data Analyst   | Co się stało?        | SQL, Tableau, Looker       |

---

## 2. Architektura Danych

### Medallion Architecture (Bronze / Silver / Gold)

Najpopularniejszy wzorzec organizacji danych w nowoczesnych data lake'ach. Trzy warstwy o rosnącej jakości danych.

```
Raw Source
    │
    ▼
┌─────────────────────────────────────┐
│  BRONZE  — surowe dane, niezmienione│  append-only, dokładna kopia źródła
│  format: Parquet / Delta            │  zachowane nawet błędne rekordy
└─────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────┐
│  SILVER  — oczyszczone, zwalidowane │  usunięte duplikaty, poprawione typy
│  format: Delta Lake                 │  zastosowane reguły biznesowe
└─────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────┐
│  GOLD    — agregacje, business marts│  gotowe do konsumpcji przez raporty
│  format: Delta Lake / Parquet       │  zdenormalizowane, zoptymalizowane
└─────────────────────────────────────┘
    │
    ▼
Dashboard / ML Model / API
```

### Data Lakehouse

Połączenie Data Lake (tani storage, dowolny format) z Data Warehouse (ACID, SQL, wydajność). Realizowane przez Delta Lake, Apache Iceberg, Apache Hudi.

```
Data Lake      = tani storage (S3/GCS) + dowolny format
Data Warehouse = ACID + SQL + wydajność (Snowflake, BigQuery)
Data Lakehouse = Data Lake + ACID transactions (Delta Lake)
```

### Lambda Architecture

Dwa równoległe pipeline'y: batch (dokładny, wolny) i streaming (szybki, przybliżony). Wyniki są mergowane w serving layer.

```
Source → Batch Layer  (Spark)   → Batch View  ┐
       → Speed Layer  (Kafka)   → Real-time   ├→ Serving Layer → Consumer
                                  View        ┘
```

### Kappa Architecture

Uproszczenie Lambda — tylko streaming, batch traktowany jako specjalny przypadek streamu. Prostszy w utrzymaniu.

---

## 3. Kluczowe Pojęcia

### Pipeline

Sekwencja kroków przetwarzania danych od źródła do celu. Każdy krok produkuje dane dla następnego.

```
Ingest → Validate → Transform → Aggregate → Load → Serve
```

### ETL vs ELT

**ETL** (Extract → Transform → Load) — transformacja przed załadowaniem. Stare podejście, dane warehouse.

**ELT** (Extract → Load → Transform) — najpierw załaduj surowe dane, potem transformuj w miejscu. Nowoczesne podejście, data lake + dbt.

```
ETL: Source → [Transform] → Warehouse
ELT: Source → Data Lake → [Transform w SQL/dbt] → Marts
```

ELT wygrywa bo storage jest tani, a transformacje in-place są szybsze i łatwiejsze do debugowania.

### Idempotency

Pipeline jest idempotentny jeśli uruchomienie go wielokrotnie na tych samych danych daje ten sam wynik. Kluczowe dla niezawodności — retry nie duplikuje danych.

```python
# NIE idempotentne — append zawsze dodaje wiersze
df.write.mode("append").save(path)

# Idempotentne — upsert zastępuje istniejące rekordy
delta_table.merge(df, "target.id = source.id") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
```

### Partycjonowanie

Podział danych na mniejsze pliki według klucza (data, kraj, kategoria). Drastycznie przyspiesza zapytania — Spark/SQL czyta tylko relevanty partycje zamiast całego datasetu.

```python
df.write \
    .partitionBy("year", "month") \
    .format("parquet") \
    .save("s3://bucket/data/")

# Wynikowa struktura:
# s3://bucket/data/year=2024/month=01/part-000.parquet
# s3://bucket/data/year=2024/month=02/part-000.parquet
```

### Schema Evolution

Zmiany schematu danych w czasie (nowe kolumny, zmiana typów) bez crashowania pipeline'u. Delta Lake obsługuje to automatycznie z `mergeSchema`.

### Data Lineage

Śledzenie skąd pochodzi każda kolumna i przez jakie transformacje przeszła. Krytyczne do debugowania i audytu. dbt generuje lineage automatycznie.

### SLA (Service Level Agreement)

Umowa ile czasu może zająć pipeline. Np. "dane muszą być dostępne do 8:00 rano". Airflow monitoruje SLA i alertuje przy przekroczeniu.

### Backfill

Retroaktywne przetworzenie historycznych danych — np. dodanie nowej kolumny do danych z ostatnich 2 lat. Airflow obsługuje catchup runs.

---

## 4. Formaty Danych

### Parquet

Kolumnowy format binarny. Standard w data engineering. Kompresja 5-10x lepsza niż CSV, zapytania kolumnowe 100x szybsze.

```
CSV (wierszowy):    id, name, price → czyta całe wiersze
Parquet (kolumnowy): czyta tylko kolumny których potrzebujesz
```

Kiedy używać: storage warstwy Bronze i Silver, dane analityczne.

### Avro

Wierszowy format binarny ze schematem. Standard dla Kafka — mały rozmiar, szybka serializacja, schema registry.

Kiedy używać: Kafka messages, event streaming, API payloads.

### Delta Lake

Warstwa ACID na Parquet. Dodaje transakcje, time travel, upsert, schema evolution. Standard Databricks, działa lokalnie.

```python
# Time Travel — odczyt stanu sprzed 7 dni
df = spark.read \
    .format("delta") \
    .option("timestampAsOf", "2024-01-01") \
    .load("s3a://silver/listings")

# Lub wersja po numerze
df = spark.read \
    .format("delta") \
    .option("versionAsOf", 42) \
    .load("s3a://silver/listings")
```

### ORC

Alternatywa dla Parquet, lepiej zintegrowana z Hive. Rzadziej spotykana poza Hadoop ekosystemem.

---

## 5. Apache Spark

### Czym jest Spark

Distributed computing framework — przetwarza dane równolegle na wielu maszynach (lub core'ach). Zastąpił Hadoop MapReduce. Działa in-memory, 100x szybszy od MapReduce.

### RDD vs DataFrame vs Dataset

```python
# RDD — niskopoziomowy, unikaj w nowym kodzie
rdd = sc.parallelize([1, 2, 3])

# DataFrame — kolumnowy, zoptymalizowany przez Catalyst
df = spark.read.csv("data.csv", header=True)

# Dataset — typowany DataFrame (tylko Scala/Java)
# W PySpark używamy DataFrame zawsze
```

### Lazy Evaluation

Spark nie wykonuje transformacji od razu — buduje plan. Dopiero `action` (count, collect, write) wyzwala obliczenia. Pozwala Sparkowi optymalizować cały plan naraz.

```python
# Transformacje — NIE wykonują się od razu (lazy)
df = spark.read.parquet("data/")
df = df.filter(df.price > 1000)
df = df.groupBy("make").avg("price")

# Action — wyzwala obliczenia
df.show()        # ← TUTAJ Spark przetwarza dane
df.count()       # ← TUTAJ
df.write.save()  # ← TUTAJ
```

### Shuffle

Najdroższy krok w Sparku — redystrybucja danych między partycjami przez sieć. Wywołana przez `groupBy`, `join`, `distinct`. Minimalizuj shuffle projektując pipeline.

```python
# Drogi join — shuffle obu datasetów
large_df.join(medium_df, "id")

# Broadcast join — mały dataset wysyłany do każdego executora, zero shuffle
from pyspark.sql import functions as F
large_df.join(F.broadcast(small_df), "id")
```

### Partitions i Parallelism

Spark dzieli dane na partycje — każda partycja to jeden task na jednym executorze. Za mało partycji = nie wykorzystujesz wszystkich core'ów. Za dużo = overhead zarządzania.

```python
# Sprawdź liczbę partycji
df.rdd.getNumPartitions()

# Zmień liczbę partycji
df.repartition(100)    # shuffle — wyrównuje rozmiar partycji
df.coalesce(10)        # bez shuffle — tylko zmniejsza liczbę
```

### Spark Structured Streaming

Mikro-batch lub continuous processing. Traktuje stream jako nieskończony DataFrame. Obsługuje Kafka, pliki, sockets.

```python
stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "topic") \
    .load()

query = stream.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/") \
    .start("/data/output/")

query.awaitTermination()
```

### Catalyst Optimizer

Spark automatycznie optymalizuje query plan — predicate pushdown (filtruje wcześniej), column pruning (czyta tylko potrzebne kolumny), join reordering. Działa automatycznie dla DataFrames i SQL.

---

## 6. Apache Kafka

### Czym jest Kafka

Distributed event streaming platform. Producenci publikują eventy do topiców, konsumenci je czytają. Eventy są persystowane na dysku — można je odczytać wielokrotnie.

```
Producer → [Topic: car_listings] → Consumer Group A (Bronze writer)
                                  → Consumer Group B (Analytics)
                                  → Consumer Group C (ML pipeline)
```

### Kluczowe pojęcia

**Topic** — nazwany kanał eventów. Odpowiednik tabeli w bazie danych.

**Partition** — topic jest podzielony na partycje. Każda partycja to ordered, immutable log. Więcej partycji = więcej równoległości.

**Offset** — pozycja eventu w partycji. Konsument zapamiętuje swój offset — przy restarcie czyta od miejsca gdzie skończył.

**Consumer Group** — grupa konsumentów czytająca ten sam topic, każdy czyta inne partycje. Automatyczny load balancing.

**Retention** — jak długo Kafka trzyma eventy. Domyślnie 7 dni. Niezależnie od czy ktoś je przeczytał.

```
Partition 0: [event_0][event_1][event_2][event_3] ← offset 4
Partition 1: [event_0][event_1]                   ← offset 2
Partition 2: [event_0][event_1][event_2]          ← offset 3
```

### Delivery Guarantees

**At-most-once** — może zgubić, nie duplikuje. Szybkie.

**At-least-once** — może duplikować, nie gubi. Standard.

**Exactly-once** — nie duplikuje, nie gubi. Najwolniejsze, wymaga transakcji.

### Schema Registry

Centralny rejestr schematów Avro/JSON dla topiców. Producenci rejestrują schemat, konsumenci go pobierają. Zapobiega breaking changes.

---

## 7. Apache Airflow

### Czym jest Airflow

Workflow orchestration — definiujesz pipeline jako DAG (Directed Acyclic Graph) w Pythonie, Airflow scheduluje i monitoruje wykonanie.

### DAG

Directed Acyclic Graph — graf zależności między taskami. Każdy task to jeden krok pipeline'u. Acyclic = brak cykli (task nie może zależeć od samego siebie).

```python
with DAG("pipeline", schedule="@daily") as dag:

    extract  = PythonOperator(task_id="extract",  python_callable=extract_fn)
    transform = PythonOperator(task_id="transform", python_callable=transform_fn)
    load     = PythonOperator(task_id="load",     python_callable=load_fn)
    notify   = BashOperator(task_id="notify",     bash_command="echo done")

    extract >> transform >> load >> notify
    #   ↑ zależności — load czeka na transform, transform na extract
```

### Operators

Gotowe bloki do budowania DAGów:

```python
PythonOperator      # uruchamia funkcję Pythona
BashOperator        # uruchamia komendę bash
SparkSubmitOperator # submituje Spark job
PostgresOperator    # wykonuje SQL na Postgres
HttpOperator        # wywołuje HTTP endpoint
BranchOperator      # warunkowe rozgałęzienie DAGu
```

### XComs

Mechanizm przekazywania małych wartości między taskami (np. liczba przetworzonych rekordów, status, ścieżka pliku). Nie do dużych danych.

```python
def upload(**context):
    path = "/data/output.parquet"
    context['ti'].xcom_push(key='output_path', value=path)

def process(**context):
    path = context['ti'].xcom_pull(key='output_path', task_ids='upload')
    print(f"Processing: {path}")
```

### Sensors

Czekają na spełnienie warunku zanim pipeline ruszy — plik pojawił się w S3, tabela jest gotowa, API odpowiada.

```python
S3KeySensor(
    task_id="wait_for_file",
    bucket_name="bronze",
    bucket_key="listings/{{ ds }}/part-*.parquet",
    timeout=3600,
    poke_interval=60,
)
```

---

## 8. dbt (data build tool)

### Czym jest dbt

SQL-first transformation framework. Piszesz SELECT, dbt ogarnia CREATE TABLE / CREATE VIEW, dependencies, testy, dokumentację i lineage.

### Model

Jeden plik `.sql` = jeden model = jedna tabela lub widok w bazie.

```sql
-- models/silver/stg_listings.sql
-- {{ }} to Jinja templating

{{ config(materialized='view') }}   -- view, table, incremental, ephemeral

SELECT
    id,
    LOWER(TRIM(make))   AS make,
    CAST(price AS FLOAT) AS price,
    year
FROM {{ source('bronze', 'listings') }}  -- referencja do źródła
WHERE price IS NOT NULL
```

### Ref i Source

```sql
-- ref() — referencja do innego modelu dbt (buduje dependency graph)
FROM {{ ref('stg_listings') }}

-- source() — referencja do surowej tabeli (spoza dbt)
FROM {{ source('bronze', 'raw_listings') }}
```

### Materialization

```sql
view         -- CREATE VIEW — zawsze aktualne, zero storage
table        -- CREATE TABLE — snapshot, szybkie zapytania
incremental  -- tylko nowe/zmienione rekordy — szybkie dla dużych tabel
ephemeral    -- CTE wbudowane w inne modele, zero storage
```

### Testy

Wbudowane testy jakości danych:

```yaml
# models/silver/stg_listings.yml
models:
  - name: stg_listings
    columns:
      - name: id
        tests: [unique, not_null]
      - name: price
        tests:
          - not_null
          - accepted_range:
              min_value: 0
              max_value: 1000000
      - name: make
        tests:
          - accepted_values:
              values: ["toyota", "ford", "honda", "chevrolet"]
```

### Makra (Jinja)

Reużywalne kawałki SQL:

```sql
-- macros/clean_string.sql
{% macro clean_string(col) %}
    LOWER(TRIM(REGEXP_REPLACE({{ col }}, '[^a-zA-Z0-9 ]', '', 'g')))
{% endmacro %}

-- użycie w modelu
SELECT {{ clean_string('make') }} AS make
```

### Incremental Models

Przetwarzaj tylko nowe rekordy — kluczowe dla dużych tabel:

```sql
{{ config(materialized='incremental', unique_key='id') }}

SELECT * FROM {{ source('bronze', 'listings') }}

{% if is_incremental() %}
    WHERE ingested_at > (SELECT MAX(ingested_at) FROM {{ this }})
{% endif %}
```

---

## 9. Object Storage

### MinIO / S3 / GCS

Tani, skalowalny storage dla plików (Parquet, Avro, Delta). Nie baza danych — brak transakcji, brak indeksów. Dostęp przez API (put/get/delete).

```
S3/GCS/MinIO + Delta Lake = Data Lakehouse
(tani storage) + (ACID layer)
```

### Konwencja nazewnictwa ścieżek

```
s3://bucket/layer/entity/partition/file.parquet

# Przykłady:
s3://bronze/listings/year=2024/month=01/part-000.parquet
s3://silver/listings_clean/year=2024/part-000.parquet
s3://gold/mart_price_by_make/part-000.parquet
```

---

## 10. Streaming vs Batch

| Cecha      | Batch                | Streaming                     |
| ---------- | -------------------- | ----------------------------- |
| Latency    | minuty–godziny       | milisekundy–sekundy           |
| Throughput | bardzo wysoki        | niższy                        |
| Złożoność  | prosta               | wysoka                        |
| Kiedy      | raporty, ML features | fraud detection, monitoring   |
| Narzędzia  | Spark, dbt, SQL      | Kafka, Spark Streaming, Flink |

### Mikro-batch

Kompromis — Spark Structured Streaming domyślnie działa w mikro-batchach (co kilka sekund). Prościej niż true streaming, wystarczające dla 90% use cases.

### Windowing

Agregacje na oknach czasowych w streamingu:

```python
from pyspark.sql import functions as F

# Tumbling window — nieprzekrywające się okna 1h
df.groupBy(
    F.window("timestamp", "1 hour"),
    "make"
).agg(F.avg("price").alias("avg_price"))

# Sliding window — okna 1h przesuwane co 30 min
df.groupBy(
    F.window("timestamp", "1 hour", "30 minutes"),
    "make"
).agg(F.avg("price").alias("avg_price"))
```

### Watermarking

Obsługa spóźnionych danych w streamingu — ile czasu czekamy na eventy które dotarły z opóźnieniem:

```python
df.withWatermark("event_time", "10 minutes") \
  .groupBy(F.window("event_time", "1 hour")) \
  .count()
```

---

## 11. Jakość Danych

### Data Validation

Sprawdzenie czy dane spełniają reguły biznesowe przed dalszym przetwarzaniem:

```python
from pydantic import BaseModel, field_validator

class ListingSchema(BaseModel):
    id: str
    price: float
    year: int

    @field_validator("price")
    def price_positive(cls, v):
        assert 500 <= v <= 500_000, f"Invalid price: {v}"
        return v
```

### Data Quality Dimensions

**Completeness** — czy brakuje wartości? (`IS NULL` checks)

**Uniqueness** — czy są duplikaty? (`COUNT DISTINCT`)

**Validity** — czy wartości są w dopuszczalnym zakresie? (`BETWEEN`, `IN`)

**Consistency** — czy dane są spójne między tabelami? (foreign key checks)

**Timeliness** — czy dane są aktualne? (lag monitoring)

### Great Expectations / dbt tests

Frameworki do deklaratywnego definiowania i testowania jakości danych. dbt tests są wystarczające dla większości projektów.

---

## 12. Praktyki Produkcyjne

### Observability — co monitorować

```
Pipeline health:   czas trwania, liczba rekordów, błędy
Data quality:      NaN rate, duplikaty, schema drift
Infrastructure:    CPU, RAM, disk, network Spark/Kafka
SLA:               czy dane są gotowe na czas
```

### Idempotency w praktyce

Każdy krok pipeline'u powinien być bezpieczny do ponownego uruchomienia. Używaj upsert zamiast insert, używaj overwrite zamiast append tam gdzie to możliwe.

### Partitioning Strategy

Partycjonuj według klucza który używasz w filtrach:

```python
# Zapytania często filtrują po dacie → partycjonuj po dacie
df.write.partitionBy("year", "month", "day").save(path)

# Zapytania filtrują po kraju → partycjonuj po kraju
df.write.partitionBy("country").save(path)

# UNIKAJ partycjonowania po high-cardinality kolumnach
# (np. user_id z milionem wartości → milion małych plików)
```

### Small Files Problem

Spark generuje wiele małych plików (jeden per partycję). Zbyt wiele małych plików = wolne zapytania (overhead metadanych). Rozwiązanie: `coalesce()` przed zapisem lub Delta Lake Auto Optimize.

```python
# Scal małe pliki przed zapisem
df.coalesce(10).write.format("delta").save(path)

# Delta Lake Auto Optimize (Databricks)
spark.sql("OPTIMIZE delta.`s3a://silver/listings`")
```

### Schema-on-Read vs Schema-on-Write

**Schema-on-Write** — schemat zdefiniowany przed zapisem (bazy relacyjne). Gwarancja jakości, mniej elastyczne.

**Schema-on-Read** — schemat interpretowany przy odczycie (data lake). Elastyczne, ryzyko schema drift.

Delta Lake = najlepsze z obu: elastyczny storage + schema enforcement przy write.

### Cost Optimization

```
Parquet compression:  5-10x mniejszy rozmiar vs CSV
Partitioning:         czytaj tylko potrzebne partycje
Broadcast joins:      eliminuj shuffle dla małych tabel
Spot instances:       tańsze compute dla Spark batch
Delta vacuuming:      usuń stare pliki (VACUUM RETAIN 7 DAYS)
```

---

## 13. Pełny Production Stack

### Modern Data Stack (2024/2025)

```
Ingestion:      Fivetran / Airbyte / własny Kafka producer
Storage:        S3 / GCS / Azure Blob + Delta Lake / Iceberg
Processing:     Apache Spark / Databricks / dbt
Orchestration:  Apache Airflow / Prefect / Dagster
Serving:        BigQuery / Snowflake / Redshift / DuckDB
BI:             Looker / Tableau / Metabase / Streamlit
Quality:        Great Expectations / dbt tests
Observability:  Datadog / Monte Carlo / custom metrics
```

### Lokalna alternatywa (portfolio / dev)

```
Ingestion:      Kafka (Docker)
Storage:        MinIO (Docker) + Delta Lake
Processing:     Spark (Docker) + dbt-spark
Orchestration:  Airflow (Docker)
Serving:        DuckDB + dbt-duckdb
BI:             Streamlit
Quality:        dbt tests + Pydantic
Observability:  Airflow logs + Spark UI
```

---

## 14. Wzorce Projektowe w DE

### Repository Pattern

Abstrakcja nad storage — kod biznesowy nie wie czy dane są w DuckDB, Spark, czy pliku.

### Factory Pattern

Tworzenie SparkSession, połączeń Kafka, klientów S3 przez fabryki — łatwe mockowanie w testach.

### Strategy Pattern

Różne strategie zapisu (append / upsert / overwrite) jako wymienne obiekty — pipeline wybiera strategię bez if/else.

### Dead Letter Queue

Rekordy których nie można przetworzyć trafiają do osobnego topicu/tabeli zamiast crashować pipeline. Przeanalizowane i ponowione później.

```
Kafka Topic (raw) → [Processor] → Topic (processed)
                               → Topic (dead_letter)  ← błędne rekordy
```

---

## 15. Słownik Terminów

| Termin            | Znaczenie                                                         |
| ----------------- | ----------------------------------------------------------------- |
| DAG               | Directed Acyclic Graph — graf zależności tasków                   |
| ELT               | Extract, Load, Transform — ładuj najpierw, transformuj później    |
| ACID              | Atomicity, Consistency, Isolation, Durability                     |
| CDC               | Change Data Capture — śledzenie zmian w bazie źródłowej           |
| SLA               | Service Level Agreement — umowa o czasie dostarczenia danych      |
| Schema drift      | Nieoczekiwana zmiana struktury danych ze źródła                   |
| Data skew         | Nierównomierne rozłożenie danych między partycjami Spark          |
| Shuffle           | Redystrybucja danych między executorami — najdroższy krok         |
| Upsert            | Update + Insert — aktualizuj jeśli istnieje, wstaw jeśli nowe     |
| Backfill          | Retroaktywne przetworzenie historycznych danych                   |
| Watermark         | Granica czasu dla spóźnionych eventów w streamingu                |
| Partition pruning | Spark czyta tylko relevanty partycje, ignoruje resztę             |
| Broadcast join    | Wysłanie małego datasetu do każdego executora — eliminuje shuffle |
| Checkpointing     | Zapisanie stanu streamu — bezpieczny restart po awarii            |
| Time Travel       | Odczyt danych w stanie z przeszłości (Delta Lake)                 |
| Lineage           | Ślad skąd pochodzi kolumna i przez jakie transformacje przeszła   |
| Idempotency       | Wielokrotne uruchomienie daje ten sam wynik                       |
| Cardinality       | Liczba unikalnych wartości w kolumnie                             |
| Compaction        | Łączenie małych plików w większe — optymalizacja wydajności       |
| Vacuum            | Usuwanie starych plików z Delta Lake po time travel retention     |
