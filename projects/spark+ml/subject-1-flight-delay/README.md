# Subject 1 · Flight Delay Analytics

[← Overview & rules](../README.md) · Files for your group's repository: [`starter/`](starter/) (`README.md` and `.gitignore`)

## Scenario and dataset

You are the data team of an airline operations centre. The Operations Director has one year of flight records, about 5.8 million flights, and asks:

> "Where and when do our delays happen?"

You answer it with Apache Spark: query the data with the PySpark DataFrame API. Calendar, Phase B, work distribution, deliverables and grading are in the [Overview & rules](../README.md) page.

| Repository | Notebook | Script (Task 8) |
| --- | --- | --- |
| `flight-delay-group-X` | `Flight_Delay_GroupX.ipynb` | `flight_delay_job.py` |

**Download:** the "2015 Flight Delays and Cancellations" dataset of the US Department of Transportation, on Kaggle at [kaggle.com/datasets/usdot/flight-delays](https://www.kaggle.com/datasets/usdot/flight-delays). A free Kaggle account is needed. It has three CSV files:

| File | Content | Size |
| --- | --- | --- |
| `flights.csv` | One row per flight in 2015 | About 5.8 million rows |
| `airlines.csv` | Airline code and name | 14 rows |
| `airports.csv` | Airport code, name, city, state | About 320 rows |

Columns of `flights.csv` used in this project:

| Column | Type | Use |
| --- | --- | --- |
| `MONTH` | int | Analysis |
| `DAY_OF_WEEK` | int (1 = Monday) | Analysis |
| `AIRLINE` | string (code) | Analysis, joins `airlines.csv` |
| `ORIGIN_AIRPORT` | string (code) | Analysis, joins `airports.csv` |
| `DESTINATION_AIRPORT` | string (code) | Analysis (routes) |
| `SCHEDULED_DEPARTURE` | int, HHMM | Extract the departure hour |
| `ARRIVAL_DELAY` | int (minutes) | Builds the label |
| `CANCELLED`, `DIVERTED` | 0/1 | Filter these flights out |

**Label:** `label = 1` if `ARRIVAL_DELAY >= 15`, otherwise `0`.

## Phase A: PySpark queries (65 points)

Use the DataFrame API (`select`, `filter`, `groupBy`, `join`, `Window`) in every task, except Task 5, which uses Spark SQL.

### Task 1: Load and clean

Task 1 reads the flights and builds `df`, the clean table that all the next tasks use. Put the three CSV files in a folder `data/` next to your notebook, and start Spark with the first cell of the [Overview & rules](../README.md) page. Then follow the four steps, and complete the lines of the code marked `...`.

1. Read `flights.csv` with the code below, then print its schema and its number of rows.
2. Count the nulls of `ARRIVAL_DELAY`, then the cancelled flights (`CANCELLED == 1`) and the diverted flights (`DIVERTED == 1`). Compare the three numbers: why do these flights have no arrival delay?
3. Remove the cancelled and the diverted flights: keep the rows where `CANCELLED == 0` and `DIVERTED == 0` (hint 4).
4. Add two columns with `withColumn`: `DEP_HOUR`, the hour of `SCHEDULED_DEPARTURE` (hint 2), and `label` (hint 5).

```python
# Start with the first cell of the "Overview & rules" page: it creates spark and imports F.

# Step 1. header=True: the first line holds the column names.
#         inferSchema=True: numbers are read as numbers (without it, every column is a string).
raw = spark.read.csv("data/flights.csv", header=True, inferSchema=True)
# raw = raw.sample(0.01, seed=42)   # uncomment while you develop: keeps 1% of the rows (hint 1)
raw.printSchema()
print("Rows before cleaning:", raw.count())

# Step 2
print("Nulls in ARRIVAL_DELAY:", raw.filter(F.col("ARRIVAL_DELAY").isNull()).count())
# ... count the cancelled and the diverted flights the same way

# Steps 3 and 4: df is the clean table that all the next tasks use
df = (raw.filter(...)                    # step 3
         .withColumn("DEP_HOUR", ...)    # step 4, hint 2
         .withColumn("label", ...)       # step 4, hint 5
         .cache())                       # keeps df in memory, because every task reuses it
print("Rows after cleaning:", df.count())
df.select("AIRLINE", "ORIGIN_AIRPORT", "SCHEDULED_DEPARTURE", "DEP_HOUR",
          "ARRIVAL_DELAY", "label").show(5)

# The two small tables, used in Task 4
airlines = spark.read.csv("data/airlines.csv", header=True, inferSchema=True)
airports = spark.read.csv("data/airports.csv", header=True, inferSchema=True)
```

**Deliverable:** the number of rows before and after cleaning, the three counts of step 2 with your explanation, and `show(5)` of `df`.

### Task 2: Select, filter and sort

- The 10 flights with the longest arrival delay: `MONTH`, `AIRLINE`, `ORIGIN_AIRPORT`, `DESTINATION_AIRPORT`, `ARRIVAL_DELAY`.
- The number of flights that left Atlanta (`ATL`) in July, and how many of them were delayed.
- The number of different origin airports. Why is it larger than the number of rows of `airports.csv`? (see hint 3)

```python
df.filter((F.col("AIRLINE") == "AA") & (F.col("DEP_HOUR") >= 18)).count()
```

**Requirement:** `select`, `filter`, `orderBy`, `limit`, `distinct` and `count` only.

### Task 3: Aggregate with groupBy

- For each airline: number of flights, average arrival delay, share of delayed flights (the average of `label`). Sort from the most to the least punctual.
- The 10 busiest origin airports: number of departures and share of delayed flights.

```python
df.groupBy("AIRLINE").agg(
    F.count("*").alias("flights"),
    F.round(F.avg("ARRIVAL_DELAY"), 2).alias("avg_delay"),
    F.round(F.avg("label"), 3).alias("delay_rate"),
).orderBy("delay_rate").show()
```

**Requirement:** `groupBy` and `agg`, with an `alias` for every computed column.

### Task 4: Join tables

- Join the airline table of Task 3 with `airlines.csv` to show the airline names (see hint 6).
- Join the flights with `airports.csv` (`ORIGIN_AIRPORT` = `IATA_CODE`), and compute the share of delayed flights per `STATE`. Show the 5 states with the highest share, among states with at least 10,000 flights.
- With a `left_anti` join, find the origin codes of `flights.csv` that are not in `airports.csv`. In which month are they?

**Requirement:** `join` with `how="inner"` and with `how="left_anti"`.

### Task 5: Airports with the most delays (Spark SQL)

- Register the clean flights and `airports.csv` as temporary views.
- With `spark.sql()`, list the 10 origin airports with the highest delay rate, among airports with at least 10,000 departures. Show the airport name and city.

**Requirement:** `spark.sql()` only, with a `JOIN`, a `GROUP BY` and a `HAVING`.

### Task 6: When do delays happen? (charts)

- The delay rate per month, and per departure hour.
- Convert each result with `.toPandas()` and draw two charts with matplotlib.

**Requirement:** explain the two charts in 3-5 sentences.

### Task 7: Window functions

- A route is a pair (`ORIGIN_AIRPORT`, `DESTINATION_AIRPORT`). Keep the routes with at least 500 flights. For each airline, show its 3 routes with the highest average arrival delay.
- For each airline, compute the delay rate of each month, and its change from the previous month. Show the 5 largest increases.

```python
w = Window.partitionBy("AIRLINE").orderBy("MONTH")
monthly.withColumn("change", F.col("delay_rate") - F.lag("delay_rate").over(w))
```

**Requirement:** `Window.partitionBy(...).orderBy(...)` with `row_number()` for the routes, and with `lag()` for the months.

## Phase B and Task 10
- **Task 9:** time your Task 3 on the full `flights.csv`, then on a Parquet copy of the same data.

### Task 10: Causes of delays

The columns `AIR_SYSTEM_DELAY`, `SECURITY_DELAY`, `AIRLINE_DELAY`, `LATE_AIRCRAFT_DELAY` and `WEATHER_DELAY` give the minutes of delay due to each cause, for flights 15 minutes late or more.

- For each airline, compute the share of delay minutes of each cause, and draw a stacked bar chart.
- Compute the same shares for the 20 busiest origin airports. Which airports suffer most from weather?

**Deliverable:** the chart, the airport table, and 3-4 sentences of conclusions.

## Hints and common pitfalls

1. **Start on a 1% sample** (`raw.sample(0.01, seed=42)`). Switch to the full file when the code works.
2. **The hour.** `SCHEDULED_DEPARTURE` is written HHMM (1430 means 14:30): use `F.floor(F.col("SCHEDULED_DEPARTURE") / 100)`.
3. **October.** In October, airports are written as 5-digit numbers instead of 3-letter codes: Task 4 finds them. In every task that uses airports or routes (Tasks 3, 5 and 7), remove October with `F.col("MONTH") != 10`, and say so in the README.
4. **Combine conditions** with `&` (and) and `|` (or), and put each condition in parentheses, as in the code of Task 2.
5. **The label must be 0 or 1:** `F.when(F.col("ARRIVAL_DELAY") >= 15, 1).otherwise(0)`.
6. **The same column name in two tables.** `airlines.csv` also has a column `AIRLINE`, which holds the name. Rename it before the join: `withColumnRenamed("AIRLINE", "AIRLINE_NAME")`.
7. **`toPandas()` only on small results.** Aggregate first, then convert.

