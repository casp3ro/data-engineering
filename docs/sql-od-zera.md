# SQL od zera — jasno i krok po kroku

> Ten dokument tłumaczy SQL **powoli i konkretnie**. Dla każdego zapytania widzisz: *jakie dane wchodzą* → *zapytanie* → *co dokładnie wychodzi*. To najszybszy sposób, żeby SQL „kliknął".

---

## Jedna myśl, która tłumaczy cały SQL

**SQL bierze tabelę (albo kilka) i zwraca nową tabelę.**

Wejście = tabela. Wynik = tabela. Zawsze. Każde zapytanie to jeden „przepis" na przerobienie tabeli wejściowej w tabelę wynikową. Jak to zrozumiesz, reszta to już tylko słówka.

I drugie: SQL jest **deklaratywny** — mówisz *co* chcesz dostać, a nie *jak* to policzyć. Nie piszesz pętli „dla każdego wiersza...". Opisujesz wynik, a silnik sam go wylicza.

---

## Nasze dwie tabelki (używamy ich przez cały dokument)

**`users`** — jeden wiersz = jeden użytkownik:

| user_id | name | country | learning_language |
|--:|---|---|---|
| 1 | Ania | PL | Spanish |
| 2 | Ben | US | French |
| 3 | Cira | US | Spanish |
| 4 | Deniz | DE | Japanese |
| 5 | Eva | PL | Spanish |

**`events`** — jeden wiersz = jedna ukończona lekcja:

| event_id | user_id | event_type |
|--:|--:|---|
| 101 | 1 | lesson_completed |
| 102 | 1 | lesson_completed |
| 103 | 2 | lesson_completed |
| 104 | 3 | lesson_completed |
| 105 | 3 | lesson_completed |
| 106 | 3 | lesson_completed |
| 107 | 5 | lesson_completed |

Zauważ: kolumna `user_id` występuje w obu tabelach — to **klucz**, który je łączy (Deniz, user 4, nie ma żadnej lekcji).

---

## 1. `SELECT` — wybierz kolumny

`SELECT` mówi *które kolumny* chcesz zobaczyć. `FROM` mówi *z której tabeli*.

```sql
SELECT name, country
FROM users;
```
**Wynik** — te same wiersze, ale tylko dwie kolumny:

| name | country |
|---|---|
| Ania | PL |
| Ben | US |
| Cira | US |
| Deniz | DE |
| Eva | PL |

`SELECT *` oznacza „wszystkie kolumny". Wygodne na start, ale w prawdziwych modelach wypisuje się kolumny jawnie (czytelność + szybkość).

---

## 2. `WHERE` — odfiltruj wiersze

`WHERE` zostawia tylko wiersze spełniające warunek. To filtr **na wierszach**.

```sql
SELECT name, country
FROM users
WHERE country = 'PL';
```
**Wynik** — zostają tylko Polacy:

| name | country |
|---|---|
| Ania | PL |
| Eva | PL |

Operatory: `=`, `!=` (różne), `>`, `<`, `>=`, `<=`, `IN ('PL','US')`, `BETWEEN`, `LIKE 'A%'` (zaczyna się na A). Łączysz je przez `AND` / `OR`.

> Tekst zawsze w `'pojedynczych cudzysłowach'`. `"podwójne"` to nazwa kolumny, nie tekst — częsty błąd na starcie.

---

## 3. `ORDER BY` i `LIMIT` — sortowanie i ucinanie

```sql
SELECT name, user_id
FROM users
ORDER BY name DESC      -- DESC = malejąco (Z→A); ASC = rosnąco (domyślne)
LIMIT 3;                -- pokaż tylko 3 pierwsze wiersze
```
**Wynik**:

| name | user_id |
|---|--:|
| Eva | 5 |
| Deniz | 4 |
| Cira | 3 |

---

## 4. Obliczenia i aliasy (`AS`)

W `SELECT` możesz liczyć nowe kolumny. `AS` nadaje im nazwę (alias).

```sql
SELECT
  name,
  country || '-' || learning_language AS combo   -- || skleja teksty
FROM users;
```
**Wynik** — `combo` to nowa, policzona kolumna:

| name | combo |
|---|---|
| Ania | PL-Spanish |
| Ben | US-French |
| Cira | US-Spanish |
| Deniz | DE-Japanese |
| Eva | PL-Spanish |

---

## 5. `GROUP BY` — grupowanie (tu jest największy próg, więc wolno)

Wyobraź sobie, że **wrzucasz wiersze do kubełków** wg wartości jakiejś kolumny. Wszystkie wiersze z `country = 'PL'` lądują w jednym kubełku, `US` w drugim itd. Potem dla **każdego kubełka** liczysz jedną liczbę (np. ile wierszy).

```sql
SELECT
  country,
  count(*) AS users     -- ile wierszy w każdym kubełku
FROM users
GROUP BY country;
```
**Wynik** — z 5 wierszy zrobiły się 3 (po jednym na kubełek):

| country | users |
|---|--:|
| PL | 2 |
| US | 2 |
| DE | 1 |

Kluczowa intuicja: po `GROUP BY` **jeden wiersz wyniku = jeden kubełek (grupa)**, nie jeden wiersz wejścia. Tabela się „zwija".

Przykład na drugiej tabeli — ile lekcji zrobił każdy user (kubełek = `user_id`):

```sql
SELECT user_id, count(*) AS lekcje
FROM events
GROUP BY user_id
ORDER BY user_id;
```
**Wynik**:

| user_id | lekcje |
|--:|--:|
| 1 | 2 |
| 2 | 1 |
| 3 | 3 |
| 5 | 1 |

### Funkcje agregujące (liczą jedną wartość z całego kubełka)
- `count(*)` — ile wierszy
- `count(kolumna)` — ile wierszy, gdzie kolumna **nie jest** pusta (NULL)
- `sum(kolumna)` — suma
- `avg(kolumna)` — średnia
- `min` / `max` — najmniejsza / największa

**Zasada, którą trzeba zapamiętać:** każda kolumna w `SELECT` musi być **albo w `GROUP BY`, albo w funkcji agregującej**. Nie możesz wybrać „luźnej" kolumny, bo silnik nie wie, którą z wielu wartości w kubełku pokazać.

---

## 6. `HAVING` — filtr **po** grupowaniu

`WHERE` filtruje pojedyncze wiersze **przed** grupowaniem.
`HAVING` filtruje **gotowe grupy** (czyli używa wyników agregacji).

```sql
SELECT user_id, count(*) AS lekcje
FROM events
GROUP BY user_id
HAVING count(*) >= 2;     -- zostaw tylko userów z 2+ lekcjami
```
**Wynik** — odpadli ci z jedną lekcją:

| user_id | lekcje |
|--:|--:|
| 1 | 2 |
| 3 | 3 |

Proste rozróżnienie:
- „pokaż lekcje z platformy iOS" → warunek na wierszu → **`WHERE`**
- „pokaż userów, którzy mają 2+ lekcji" → warunek na liczbie z grupy → **`HAVING`**

---

## 7. Dlaczego alias czasem „nie działa" — kolejność wykonania

Piszesz `SELECT` na samej górze, ale silnik **wykonuje** części w innej kolejności:

```
FROM   →  WHERE  →  GROUP BY  →  HAVING  →  SELECT  →  ORDER BY  →  LIMIT
(skąd)   (filtr   (kubełki)   (filtr     (kolumny   (sortuj)    (utnij)
          wierszy)            grup)       i aliasy)
```

To tłumaczy typowy błąd:

```sql
SELECT user_id, count(*) AS lekcje
FROM events
WHERE lekcje > 2        -- ❌ BŁĄD: "lekcje" powstaje dopiero w SELECT,
GROUP BY user_id;       --    a WHERE wykonuje się WCZEŚNIEJ
```
W momencie `WHERE` alias `lekcje` jeszcze nie istnieje. Dlatego filtr agregatu idzie do `HAVING` (które wykonuje się po grupowaniu). Nie musisz uczyć się tego na pamięć — wystarczy, że gdy coś „nie widzi" aliasu, wiesz dlaczego.

---

## 8. `JOIN` — łączenie dwóch tabel

`JOIN` **dokłada kolumny** z drugiej tabeli, dopasowując wiersze po wspólnym kluczu (u nas `user_id`).

### `INNER JOIN` — tylko pasujące pary
Zostają wiersze, które mają dopasowanie po **obu** stronach.

```sql
SELECT u.name, e.event_id
FROM users u
INNER JOIN events e ON e.user_id = u.user_id;
```
**Wynik** — Deniz (user 4) znika, bo nie ma żadnej lekcji:

| name | event_id |
|---|--:|
| Ania | 101 |
| Ania | 102 |
| Ben | 103 |
| Cira | 104 |
| Cira | 105 |
| Cira | 106 |
| Eva | 107 |

(`u` i `e` to **aliasy tabel** — skrót, żeby nie pisać pełnych nazw. `u.name` = kolumna `name` z `users`.)

### `LEFT JOIN` — zachowaj WSZYSTKO z lewej tabeli
Wszyscy userzy zostają; jeśli ktoś nie ma pary po prawej, brakujące kolumny są puste (`NULL`).

```sql
SELECT u.name, e.event_id
FROM users u
LEFT JOIN events e ON e.user_id = u.user_id;
```
**Wynik** — Deniz zostaje, ale z pustym `event_id`:

| name | event_id |
|---|--:|
| Ania | 101 |
| Ania | 102 |
| Ben | 103 |
| Cira | 104 |
| Cira | 105 |
| Cira | 106 |
| Deniz | *NULL* |
| Eva | 107 |

To najważniejsza różnica:
- **`INNER JOIN`** — „pokaż userów, którzy mają lekcje" (gubi Deniza).
- **`LEFT JOIN`** — „pokaż wszystkich userów, a lekcje jeśli są" (zostawia Deniza).

### JOIN + GROUP BY = realne pytanie biznesowe
„Ile lekcji ma każdy user — łącznie z tymi, co mają zero?"

```sql
SELECT u.name, count(e.event_id) AS lekcje
FROM users u
LEFT JOIN events e ON e.user_id = u.user_id
GROUP BY u.name
ORDER BY lekcje DESC;
```
**Wynik**:

| name | lekcje |
|---|--:|
| Cira | 3 |
| Ania | 2 |
| Ben | 1 |
| Eva | 1 |
| Deniz | 0 |

Dlaczego Deniz ma **0**, a nie 1? Bo użyliśmy `count(e.event_id)` — liczy tylko **niepuste** wartości, a Deniz ma `NULL`. Gdybyśmy dali `count(*)`, policzyłby jego pusty wiersz jako 1. Mała rzecz, duża różnica — i częste źródło błędów w raportach.

---

## 9. `WITH` (CTE) — buduj zapytanie po schodkach

CTE to **tymczasowa, nazwana tabela** zdefiniowana na początku zapytania. Zamiast pisać jednego zagnieżdżonego potwora, układasz logikę w czytelne kroki.

Pytanie: „ilu userów ma 1 lekcję, ilu 2, ilu 3?" — czyli rozkład. To dwa kroki:

```sql
WITH per_user AS (                       -- KROK 1: ile lekcji ma każdy user
  SELECT user_id, count(*) AS lekcje
  FROM events
  GROUP BY user_id
)
SELECT lekcje, count(*) AS ilu_userow    -- KROK 2: pogrupuj userów wg liczby lekcji
FROM per_user
GROUP BY lekcje
ORDER BY lekcje;
```
**Wynik**:

| lekcje | ilu_userow |
|--:|--:|
| 1 | 2 |
| 2 | 1 |
| 3 | 1 |

Czytasz to z góry na dół jak akapity. **Czytelne 3 CTE > jedno sprytne zapytanie na pół ekranu.** To jest styl, którego oczekuje się w prawdziwej pracy.

---

## 10. Pierwszy „wyższy bieg" — `ROW_NUMBER` (intuicja)

Czasem chcesz coś policzyć **bez zwijania tabeli** — każdy wiersz ma zostać, ale dostać dodatkową informację. Do tego służą *window functions*. Najważniejsza na start: `ROW_NUMBER` — numeruje wiersze w obrębie grupy.

```sql
SELECT
  event_id, user_id,
  row_number() OVER (PARTITION BY user_id ORDER BY event_id) AS nr
FROM events;
```
**Wynik** — numeracja **restartuje** dla każdego usera (`PARTITION BY user_id`):

| event_id | user_id | nr |
|--:|--:|--:|
| 101 | 1 | 1 |
| 102 | 1 | 2 |
| 103 | 2 | 1 |
| 104 | 3 | 1 |
| 105 | 3 | 2 |
| 106 | 3 | 3 |
| 107 | 5 | 1 |

Po co to? Najczęstsze użycie: **„zostaw tylko pierwszy wiersz każdej grupy"** — np. pierwsza lekcja usera, albo usunięcie duplikatów. Dopisujesz `WHERE nr = 1` i gotowe.

Tu się na razie zatrzymujemy — pełne window functions (`LAG`, `SUM() OVER`, ramki) są w dokumencie referencyjnym i przećwiczymy je na żywo w projekcie. To naturalny kolejny krok, gdy fundamenty z punktów 1–9 będą wygodne.

---

## Mapa myślowa (cały SQL na jednym obrazku)

```
FROM      ← weź tabelę(e)
JOIN      ← dołóż kolumny z innej tabeli (po kluczu)
WHERE     ← wyrzuć niepasujące wiersze
GROUP BY  ← wrzuć wiersze do kubełków
  (agregaty: count/sum/avg liczą jedną wartość na kubełek)
HAVING    ← wyrzuć niepasujące kubełki
SELECT    ← wybierz/policz kolumny do pokazania
ORDER BY  ← posortuj
LIMIT     ← utnij
```

Jeśli czujesz punkty 1–9, jesteś gotowy na M1 w praktyce — i masz fundament, na którym stoi cały dbt i modelowanie danych. Czytaj ten dokument z bazą Lingua otwartą obok i przepisuj zapytania samodzielnie (nie kopiuj) — od tego SQL wchodzi najtrwalej.
