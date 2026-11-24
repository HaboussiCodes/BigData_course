# Subject 2 · NYC Taxi Trip Time

[← Overview & rules](../README.md) · Files for your group's repository: [`starter/`](starter/) (`README.md` and `.gitignore`)

## Scenario and dataset

You are the data team of a New York taxi app. The product manager wants to understand how long trips take, and asks:

> "How does traffic change across the city and during the day?"

You answer with Spark: query three months of yellow-taxi trips with the PySpark DataFrame API. Calendar, Phase B, work distribution, deliverables and grading are in the [Overview & rules](../README.md) page.

| Repository | Notebook | Script (Task 8) |
| --- | --- | --- |
| `nyc-taxi-group-X` | `NYC_Taxi_GroupX.ipynb` | `taxi_duration_job.py` |

**Download:** the trip records of the NYC Taxi and Limousine Commission (TLC), from the [TLC Trip Record Data page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). No account is needed. The files are in Parquet, one per month. Use January to March 2024, plus the zone lookup table:

```bash
mkdir -p data && cd data
for m in 01 02 03; do
  curl -O https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-$m.parquet
done
curl -O https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

On Windows, run these commands in Git Bash, which comes with Git, or open the links in a browser.

| File | Content | Size |
| --- | --- | --- |
| `yellow_tripdata_2024-01.parquet` to `-03` | One row per trip | About 3 million rows per month |
| `taxi_zone_lookup.csv` | `LocationID`, `Borough`, `Zone`, `service_zone` | 265 rows |

Columns used in this project (all columns are described in the [yellow trips data dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)):

| Column | Type | Use |
| --- | --- | --- |
| `tpep_pickup_datetime` | timestamp | Hour, day of week, duration, analysis |
| `tpep_dropoff_datetime` | timestamp | Duration |
| `PULocationID`, `DOLocationID` | int (zone ID) | Joins with the zone table, analysis |
| `trip_distance` | double (miles) | Analysis |
| `passenger_count` | double | Analysis |
| `fare_amount`, `tip_amount`, `total_amount` | double (dollars) | Analysis |
| `payment_type` | int: 1 = credit card, 2 = cash (other codes in the data dictionary) | Analysis |

**Trip duration:** `duration_min`, the time from pickup to dropoff in minutes, added in Task 1 (hint 2).

## Phase A: PySpark queries (65 points)

Use the DataFrame API (`select`, `filter`, `groupBy`, `join`, `Window`) in every task, except Task 5, which uses Spark SQL.

### Task 1: Load and clean

Task 1 reads the three months of trips and builds `df`, the clean table that all the next tasks use. Put the four files in a folder `data/` next to your notebook, and start Spark with the first cell of the [Overview & rules](../README.md) page. Then follow the three steps, and complete the lines of the code marked `...`.

1. Read the three Parquet files with the code below: the `*` in the path matches the three months, so they become one DataFrame. Print its schema and its number of rows.
2. Add three columns with `withColumn`: `duration_min` (hint 2), the pickup `hour` (`F.hour`) and `day_of_week` (`F.dayofweek`, hint 6).
3. Keep only the trips with a pickup date from January to March 2024 (hint 3), a duration of more than 1 minute and at most 180 minutes, and a distance of more than 0 and at most 100 miles. The code gives the date condition: write the two others the same way.

```python
# Start with the first cell of the "Overview & rules" page: it creates spark and imports F.

# Step 1. A Parquet file stores the column names and types: no header or inferSchema needed.
raw = spark.read.parquet("data/yellow_tripdata_2024-*.parquet")    # if this fails, see hint 4
# raw = raw.sample(0.01, seed=42)   # uncomment while you develop: keeps 1% of the trips (hint 1)
raw.printSchema()
print("Rows before cleaning:", raw.count())

# Steps 2 and 3: df is the clean table that all the next tasks use
df = (raw.withColumn("duration_min", ...)                     # step 2, hint 2
         .withColumn("hour", F.hour("tpep_pickup_datetime"))
         .withColumn("day_of_week", ...)                      # step 2, hint 6
         .filter((F.year("tpep_pickup_datetime") == 2024) & (F.month("tpep_pickup_datetime") <= 3))
         .filter(...)                                         # step 3: duration and distance
         .cache())                                            # keeps df in memory: every task reuses it
print("Rows after cleaning:", df.count())
df.select("tpep_pickup_datetime", "trip_distance", "duration_min", "hour", "day_of_week").show(5)

# The zone table, used in Task 4
zones = spark.read.csv("data/taxi_zone_lookup.csv", header=True, inferSchema=True)
```

**Deliverable:** the number of rows before and after cleaning, and `show(5)` of the clean data.

### Task 2: Select, filter and sort

- The 10 longest trips by distance: pickup time, pickup and dropoff zone IDs, distance, duration and total amount.
- The number of trips with more than 4 passengers, and the number of trips paid in cash (`payment_type` = 2).
- The number of trips with a tip of more than 20 dollars.

```python
df.filter((F.col("trip_distance") > 20) & (F.col("passenger_count") == 1)).count()
```

**Requirement:** `select`, `filter`, `orderBy`, `limit` and `count` only.

### Task 3: Aggregate with groupBy

- For each payment type: number of trips, average fare, average tip. Why is the average tip of cash trips close to 0?
- For each day of the week: number of trips and average duration.

**Requirement:** `groupBy` and `agg`, with an `alias` for every computed column.

### Task 4: Join tables

- Join the trips with `taxi_zone_lookup.csv` on `PULocationID` to get the pickup borough. For each borough: number of trips, average duration, average distance.
- Join a second time, on `DOLocationID`, to get the dropoff borough (see hint 5). Show the 10 most frequent pairs (pickup borough, dropoff borough).
- With a `left_anti` join, find the zones of the lookup table where no trip started in the three months.

**Requirement:** `join` with `how="inner"` and with `how="left_anti"`.

### Task 5: Busiest routes (Spark SQL)

- With `spark.sql()`, list the 10 pickup-dropoff zone pairs with the most trips, among pairs with at least 1,000 trips. Show both zone names, the number of trips and the average duration.

**Requirement:** `spark.sql()` only, with two `JOIN`s on the zone table, a `GROUP BY` and a `HAVING`.

### Task 6: When is the city busy? (charts)

- The number of trips per pickup hour, and the average duration per pickup hour.
- Convert each result with `.toPandas()` and draw two charts with matplotlib.

**Requirement:** explain the two charts in 3-5 sentences.

### Task 7: Window functions

- For each borough, show its 3 pickup zones with the most trips.
- Count the trips per day (`F.to_date("tpep_pickup_datetime")`), and add the average of the last 7 days:

```python
w = Window.orderBy("date").rowsBetween(-6, 0)
daily.withColumn("avg_7_days", F.avg("trips").over(w))
```

**Requirement:** `Window.partitionBy(...).orderBy(...)` with `row_number()` for the zones, and `rowsBetween` for the moving average. Which days are far below their 7-day average, and why?

## Phase B and Task 8
- **Task 8:** time your Task 3 on the three months. The data already comes as Parquet, so compare it with a CSV copy.

### Task 9: Busy hours of each zone

- For the 20 busiest pickup zones, count the trips per hour: `groupBy("Zone").pivot("hour").count()`.
- Draw it as a heatmap (zones × hours) with matplotlib `imshow`.
- Which zones are busy at night? Which ones in the morning?

**Deliverable:** the heatmap and 3-4 sentences of conclusions.

## Hints and common pitfalls

1. **Start on a 1% sample** (`raw.sample(0.01, seed=42)`). Switch to the full data when the code works.
2. **The duration.** `(F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60` gives minutes.
3. **Trips from other months.** Each monthly file holds a few trips dated in another month or year. Remove them in Task 1.
4. **Schemas can differ between months.** If reading the three files at once fails, read each month separately and combine them with `unionByName`.
5. **The same columns twice.** When you join the zone table twice, rename its columns each time (for example `Borough` becomes `pu_borough`, then `do_borough`), or Spark cannot tell them apart.
6. **Days of the week.** `F.dayofweek` gives 1 for Sunday and 7 for Saturday.
7. **`toPandas()` only on small results.** Aggregate first, then convert.
