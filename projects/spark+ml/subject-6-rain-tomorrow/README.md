# Subject 6 · Rain Tomorrow

[← Overview & rules](../README.md) · Files for your group's repository: [`starter/`](starter/) (`README.md` and `.gitignore`)

## Scenario and dataset

You are the data team of a weather service. The director asks:

> "How do climates differ around the world and across Algeria?"

You answer with Spark: query two years of daily observations from thousands of weather stations with the PySpark DataFrame API. Calendar, Phase B, work distribution, deliverables and grading are in the [Overview & rules](../README.md) page.

| Repository | Notebook |
| --- | --- | --- |
| `rain-tomorrow-group-X` | `Rain_Tomorrow_GroupX.ipynb` |
**Download:** the Global Surface Summary of the Day (GSOD) of NOAA, from the [NCEI archive](https://www.ncei.noaa.gov/data/global-summary-of-the-day/archive/): one `.tar.gz` per year, holding one CSV file per station. Also download the station list. No account is needed:

```bash
mkdir -p data/gsod && cd data/gsod
for y in 2023 2024; do
  curl -O https://www.ncei.noaa.gov/data/global-summary-of-the-day/archive/$y.tar.gz
  mkdir -p $y && tar -xzf $y.tar.gz -C $y
done
curl -O https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv
```

On Windows, run these commands in Git Bash, which comes with Git and includes `curl` and `tar`.

| File | Content |
| --- | --- |
| `2023/*.csv`, `2024/*.csv` | One file per station and year, one row per day: millions of rows per year |
| `isd-history.csv` | One row per station: `USAF`, `WBAN`, `STATION NAME`, `CTRY` (country code), `LAT`, `LON`, `ELEV(M)` |

Columns used in this project (the [GSOD readme](https://git.lsit.ucsb.edu/publicdata/NCEI-NOAA/src/branch/main/global-summary-of-the-day/doc/readme.txt) describes them all):

| Column | Unit and missing code | Use |
| --- | --- | --- |
| `STATION` | 11 digits: `USAF` followed by `WBAN` | Key, joins the station list |
| `DATE` | date | Analysis |
| `LATITUDE`, `LONGITUDE`, `NAME` | degrees, text | Analysis |
| `TEMP`, `DEWP`, `MAX` | °F, missing = 9999.9 | Analysis |
| `SLP` | sea-level pressure in mbar, missing = 9999.9 | Analysis |
| `WDSP` | wind speed in knots, missing = 999.9 | Analysis |
| `PRCP` | inches, missing = 99.99 | Builds the rain column |
| `FRSHTT` | 6 digits: fog, rain, snow, hail, thunder, tornado (1 = yes) | Builds the rain column |

**Rainy day:** `rain` = 1 when `PRCP` > 0 (and not missing) or the second digit of `FRSHTT` is 1. `rain` = 0 when `PRCP` = 0 and that digit is 0. Otherwise `rain` is unknown (null).

## Phase A: PySpark queries

Use the DataFrame API (`select`, `filter`, `groupBy`, `join`, `Window`) in every task, except Task 5, which uses Spark SQL.

### Task 1: Load and clean

Task 1 reads the station files of 2023 and 2024, cleans them and saves a Parquet copy, which all the next tasks read (hint 7). The download commands above put the files in `data/gsod/`. Start Spark with the first cell of the [Overview & rules](../README.md) page, then follow the five steps, and complete the lines of the code marked `...`.

1. Read all the station files at once with the code below. Do not use `inferSchema`: every column stays a string, so the station IDs keep their leading zeros (hint 1). Print the number of rows and the number of stations.
2. Cast `TEMP`, `DEWP`, `MAX`, `SLP`, `WDSP`, `PRCP`, `LATITUDE` and `LONGITUDE` to double, and replace the missing-value codes (9999.9, 999.9, 99.99) with null (hint 2). The loop in the code does it.
3. Convert `DATE` to a date, and `TEMP`, `DEWP` and `MAX` from °F to °C (hint 5).
4. Add the `rain` column, as defined above (hint 4).
5. Save the result as Parquet, and read it back into `df`. From then on, start your notebook from this Parquet copy.

```python
# Start with the first cell of the "Overview & rules" page: it creates spark and imports F.

# Step 1. The first * is the year folder, the second * the station file.
#         No inferSchema: every column stays a string (hint 1).
raw = spark.read.csv("data/gsod/*/*.csv", header=True)
print("Rows:", raw.count())
print("Stations:", raw.select("STATION").distinct().count())

# Step 2. Each measure and its missing-value code (hint 2)
MISSING = {"TEMP": 9999.9, "DEWP": 9999.9, "MAX": 9999.9, "SLP": 9999.9, "WDSP": 999.9, "PRCP": 99.99}
clean = raw
for name, code in MISSING.items():
    clean = clean.withColumn(name, F.col(name).cast("double"))
    clean = clean.withColumn(name, F.when(F.col(name) == code, None).otherwise(F.col(name)))
clean = (clean.withColumn("LATITUDE", F.col("LATITUDE").cast("double"))
              .withColumn("LONGITUDE", F.col("LONGITUDE").cast("double")))

# Steps 3 and 4
clean = (clean.withColumn("DATE", F.to_date("DATE"))
              .withColumn("TEMP", ...)        # step 3, hint 5: the same for DEWP and MAX
              .withColumn("rain", ...))       # step 4, hint 4

# Step 5. Save once. Next time, start the notebook from the read below (hint 7).
clean.write.mode("overwrite").parquet("data/gsod_clean.parquet")
df = spark.read.parquet("data/gsod_clean.parquet")
# The nulls of each column
df.select([F.count(F.when(F.col(c).isNull(), 1)).alias(c) for c in df.columns]).show()
df.select("STATION", "NAME", "DATE", "TEMP", "MAX", "PRCP", "FRSHTT", "rain").show(5)

# The station list, used in Task 4: also read without inferSchema (hint 1)
stations = spark.read.csv("data/gsod/isd-history.csv", header=True)
```

**Deliverable:** the number of rows and of stations, the number of nulls per column, and `show(5)`.

### Task 2: Select, filter and sort

- The 10 hottest days in the data (the highest `MAX`): station `NAME`, `DATE` and `MAX` in °C. Are they plausible?
- The number of station-days with a maximum above 45 °C, and the number of rainy days in the whole data.
- The stations whose `NAME` contains "ALGER" or "BEIDA": their `STATION` code and coordinates. You need Algiers in Task 6.

```python
df.filter((F.col("max_c") > 40) & (F.col("LATITUDE") > 30)).count()
```

**Requirement:** `select`, `filter`, `orderBy`, `limit`, `distinct` and `count` only.

### Task 3: Aggregate with groupBy

- For each station: number of days, average temperature, share of rainy days. How many stations have fewer than 300 days of data?
- For each year: number of stations and number of rows.

**Requirement:** `groupBy` and `agg`, with an `alias` for every computed column.

### Task 4: Join tables

- Join the observations with `isd-history.csv` to get the country code `CTRY`. The join key is `USAF` followed by `WBAN` (see hint 3).
- For each country with at least 10 stations: number of stations, average temperature, share of rainy days. Show the 10 rainiest and the 10 driest countries. Where does Algeria (`CTRY = "AG"`) stand?
- With a `left_anti` join, count the stations of the observations that are missing from `isd-history.csv`.

**Requirement:** `join` with `how="inner"` and with `how="left_anti"`.

### Task 5: Algeria from north to south (Spark SQL)

- With `spark.sql()`, list the Algerian stations with at least 300 days of data: name, latitude, number of days, average temperature, number of rainy days. Sort them from north to south.

**Requirement:** `spark.sql()` only, with a `JOIN`, a `GROUP BY` and a `HAVING`.

### Task 6: Seasons in Algiers (charts)

- With the Algiers airport station found in Task 2: its average temperature per month, and its share of rainy days per month.
- Convert each result with `.toPandas()` and draw two charts with matplotlib.

**Requirement:** explain the two charts in 3-5 sentences. When is the rainy season?

### Task 7: Window functions

- For each Algerian station, show its 3 hottest days (the highest `MAX`).
- For the Algiers station, compute the change of temperature from one day to the next, and show the 5 largest drops. In which season are they?

```python
w = Window.partitionBy("STATION").orderBy("date")
algiers.withColumn("change", F.col("temp_c") - F.lag("temp_c").over(w))
```

**Requirement:** `row_number()` over `Window.partitionBy(...).orderBy(...)` for the hottest days, and `lag()` for the daily change.

## Phase B 

- **Task 8:** time your Task 3 on the thousands of small CSV files, then on your Parquet copy. Print the number of partitions of each (`df.rdd.getNumPartitions()`).

### Task 9: Climate map

- Build one row per station with at least 300 days of data: latitude, longitude, average temperature, share of rainy days.
- Draw the stations with longitude on x and latitude on y, coloured by average temperature (`plt.scatter(..., c=...)` and `plt.colorbar()`).
- Draw the same map coloured by the share of rainy days. Where are the rainiest regions? Where does Algeria stand?

**Deliverable:** the two maps and 3-4 sentences of conclusions.

## Hints and common pitfalls

1. **Read everything as strings first.** With `inferSchema=True`, `STATION` and `FRSHTT` lose their leading zeros (`010000` becomes `10000`). The same happens to `USAF` and `WBAN` in `isd-history.csv`.
2. **Missing values are numbers.** 9999.9, 999.9 and 99.99 mean "missing". Left as they are, they ruin every average.
3. **The station key.** `F.concat("USAF", "WBAN")` in `isd-history.csv` gives the `STATION` of the observations.
4. **The rain digit.** `F.substring("FRSHTT", 2, 1)` gives the second digit (positions start at 1).
5. **Units.** °C = (°F - 32) × 5 / 9.
6. **Missing days.** Stations skip days, so `lead()` and `lag()` do not always give the next or the previous day: check `F.datediff`.
7. **Many small files are slow.** Save to Parquet once in Task 1, and read the Parquet afterwards.
