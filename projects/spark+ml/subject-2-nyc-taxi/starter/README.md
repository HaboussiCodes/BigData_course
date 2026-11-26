# NYC Taxi Trip Time · Group X

**Big Data with PySpark · group project · Subject 2**

> Replace **X** with your group number, in this file and in the file names.

You are the data team of a New York taxi app. The product manager wants to understand how long trips take, and asks:

> *"How does traffic change across the city and during the day?"*

You answer with Spark: query three months of yellow-taxi trips with the PySpark DataFrame API.

This README has two parts. **Part 1** tells you how to start: the team, the data, the setup, the files, the tasks and the rules. **Part 2** is your project report: fill it in as you finish each task. The full instructions are in your teacher's repository: [Overview & rules](https://github.com/HaboussiCodes/BigData_course/tree/main/projects/spark%2Bml) and [Subject 2](https://github.com/HaboussiCodes/BigData_course/tree/main/projects/spark%2Bml/subject-2-nyc-taxi).

# Part 1 · Getting started

## Team

| Member | Name | Student ID | GitHub username | Tasks |
|---|---|---|---|---|
| A | | | | 1-3: loading and cleaning, filtering and sorting, aggregation with `groupBy` |
| B | | | | 4-5: joins, Spark SQL |
| C | | | | 6-7: charts, window functions |
| D | | | | 8: performance experiment |
| E | | | | 9 |

Your teacher gives each member one block of tasks. Each member commits the code of their own block, but everyone must understand the whole project: at the oral check, the teacher can ask any member about any task.

### Week 1 checklist

- [ ] Create the repository **`nyc-taxi-group-X`** on GitHub, and add all the members and your teacher (`HaboussiCodes`).
- [ ] Commit this `README.md` and the `.gitignore`.
- [ ] Download the data into `data/` (section 1).
- [ ] Every member installs the software and passes the quick check (section 2).
- [ ] Member A pushes Task 1 first: all the other tasks start from its clean data.

## 1. Get the data

The trip records of the NYC Taxi and Limousine Commission (TLC), from the [TLC Trip Record Data page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). No account is needed. The files are in Parquet, one per month: use January to March 2024, plus the zone lookup table. Run these commands from the repository folder; on Windows, run them in Git Bash, which comes with Git, or open the links in a browser:

```bash
mkdir -p data && cd data
for m in 01 02 03; do
  curl -O https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-$m.parquet
done
curl -O https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
cd ..
```

| File | Content | Size |
|---|---|---|
| `yellow_tripdata_2024-01.parquet` to `-03` | One row per trip | About 3 million rows per month |
| `taxi_zone_lookup.csv` | `LocationID`, `Borough`, `Zone`, `service_zone` | 265 rows |

All the columns are described in the [yellow trips data dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf).

**Never commit the data:** `data/` is in `.gitignore`. Keep about 5 GB free for the raw files and the Parquet copies. If the connection is slow or blocked, one member downloads the files once and shares them inside the group (USB stick or shared drive), never through GitHub.

## 2. Set up your computer

The whole project runs on a laptop in `local[*]` mode: no cluster is needed. Every member installs the same versions.

| Software | Version | Install |
|---|---|---|
| Java (JDK) | 17, not 21 or later | [Temurin 17 LTS](https://adoptium.net) |
| Python | 3.11 | [python.org/downloads](https://www.python.org/downloads/) |
| PySpark and libraries | PySpark 3.5.x, pandas below 3 | `pip install "pyspark==3.5.*" "pandas<3" matplotlib pyarrow jupyter` |

**Windows only:** copy `winutils.exe` and `hadoop.dll` from a `hadoop-3.3.x/bin` folder of [cdarlint/winutils](https://github.com/cdarlint/winutils) into `C:\hadoop\bin`, set `HADOOP_HOME=C:\hadoop`, add `%HADOOP_HOME%\bin` to `PATH`, and open a new terminal.

**Quick check** (every member, in a terminal):

```bash
java -version      # must print 17.x
python --version   # 3.11.x
python -c "from pyspark.sql import SparkSession; s = SparkSession.builder.master('local[*]').getOrCreate(); print('Spark', s.version, 'works'); s.stop()"
```

**First cell of the notebook:**

```python
import os, sys
os.environ["PYSPARK_PYTHON"] = sys.executable   # avoids worker errors on Windows
from pyspark.sql import SparkSession, functions as F
spark = (SparkSession.builder.master("local[*]").appName("project")
         .config("spark.driver.memory", "4g")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
```

The [Setup section](https://github.com/HaboussiCodes/BigData_course/tree/main/projects/spark%2Bml#setup) of the Overview & rules lists the common errors and their fixes (wrong Java version, `winutils`, `OutOfMemoryError`…).

## 3. Repository layout

```text
nyc-taxi-group-X/
├── README.md               <- this file, completed as your report
├── NYC_Taxi_GroupX.ipynb   <- Tasks 1-9
├── output/                 <- charts, logs, screenshots
├── data/                   <- the dataset, never committed
└── .gitignore
```

The notebook has one markdown header per task, with the code, the outputs, the charts and your written analysis. It must run from top to bottom without errors. Write the Parquet copies of Tasks 1 and 9 inside `data/`, never in `output/`.

## 4. How to run

**Notebook:** start `jupyter notebook`, open `NYC_Taxi_GroupX.ipynb`, then restart the kernel and run all the cells.

**Script (Task 8):** from the repository folder (Git Bash on Windows), save the full terminal output as evidence:

```bash
mkdir -p output/spark_submit
spark-submit --master "local[*]" --driver-memory 4g taxi_duration_job.py data/ 2>&1 | tee output/spark_submit/run.log
```

The script creates its own `SparkSession`, reads the data path from the command line (`sys.argv[1]`), and ends with `spark.stop()`.

## 5. Tasks, owners and deadlines

| Task | What you do | Member | Push by |
|---|---|---|---|
| 1 | Load and clean: read the three Parquet files, add `duration_min`, `hour` and `day_of_week`, keep valid trips of January to March 2024 | - | - |
| 2 | Select, filter, sort: the longest trips, trips with more than 4 passengers, cash trips, tips over 20 dollars | - | - |
| 3 | Aggregate: trips, fare and tip per payment type, trips and duration per day of the week | - | - |
| 4 | Joins: pickup boroughs, pickup-dropoff borough pairs, zones with no pickup (`left_anti`) | - | - |
| 5 | Spark SQL: the 10 busiest zone pairs | - | - |
| 6 | Charts: trips and average duration per pickup hour | - | - |
| 7 | Window functions: the top 3 zones of each borough, the 7-day moving average of daily trips | - | - |
| 8 | Performance: `local[1]` against `local[*]`, Parquet against a CSV copy | - | - |
| 9 | Extension: the busy hours of each zone (heatmap) | - | - |

**Points (out of 15):**

## 6. Rules

- **Branches and Pull Requests:** one branch per task (`git checkout -b task6-yourname`), small commits as you progress, then a Pull Request to `main`, reviewed and merged by another member. One large commit at the end does not count.
- **Your name on your code:** every notebook cell and every script section starts with a header. If your name appears on no code, you are treated as a non-contributor.

  ```python
  # ============================================
  # Task 3: Aggregation with groupBy
  # Author: First Last (ID: 12345)
  # ============================================
  ```

- **Teacher access:** add your teacher (`HaboussiCodes`) as a collaborator of the repository during week 1.
- **Data:** never commit the data, the Parquet copies or any large file. GitHub refuses files larger than 100 MB.
- **AI tools:** allowed for debugging errors, conceptual explanations and comments. Generating a whole task or the whole pipeline is not allowed. At the oral check, a student who cannot explain their own code gets zero.
- **Oral check:** in the first lab session after the deadline, 15 minutes per group: a 5-minute demo of the results, then each member explains a part of their own code.

---

# Part 2 · Project report

> Fill in every section before the deadline. Replace the lines in *italics* and fill the empty cells. The numbers must be the same as in your notebook outputs. Save your charts in `output/`: the image links below already point there, so keep the same file names or change the links. The README report is worth 10 points: all sections present, clear conclusions.

## Executive summary

*2-3 sentences that answer the question of the scenario.*

## Phase A: main findings (Tasks 1-7)

### Task 1 · Load and clean

| Question | Answer |
|---|---|
| Rows before cleaning | |
| Rows after cleaning | |

### Task 2 · Select, filter and sort

*The 10 longest trips by distance (paste the table).*

| Question | Answer |
|---|---|
| Trips with more than 4 passengers | |
| Trips paid in cash (`payment_type` = 2) | |
| Trips with a tip of more than 20 dollars | |

### Task 3 · Aggregate with groupBy

*The table per payment type (trips, average fare, average tip) and the table per day of the week (trips, average duration).*

| Question | Answer |
|---|---|
| Why is the average tip of cash trips close to 0? | |

### Task 4 · Join tables

*The table per pickup borough (trips, average duration, average distance) and the 10 most frequent pairs (pickup borough, dropoff borough).*

| Question | Answer |
|---|---|
| Zones of the lookup table where no trip started (`left_anti`) | |

### Task 5 · Busiest routes (Spark SQL)

*Your SQL query, and its result: the 10 pickup-dropoff zone pairs with the most trips (at least 1,000), with both zone names, the number of trips and the average duration.*

### Task 6 · When is the city busy? (charts)

![Trips per pickup hour](output/task6_trips_per_hour.png)
![Average duration per pickup hour](output/task6_duration_per_hour.png)

*3-5 sentences that explain the two charts.*

### Task 7 · Window functions

*The 3 pickup zones with the most trips in each borough, and the trips per day with their 7-day moving average.*

*Which days are far below their 7-day average, and why?*

## Phase B: execution and performance (Tasks 8-9)

### Task 8 · Performance experiment

*Run time of Task 3 on the full data. Run each setting 3 times and give the average. Time an action (`count()`, `collect()` or a write), not a transformation.*

| Setting | Run 1 (s) | Run 2 (s) | Run 3 (s) | Average (s) |
|---|---|---|---|---|
| `local[1]`, Parquet | | | | |
| `local[*]`, Parquet | | | | |
| `local[*]`, CSV copy | | | | |

*One or two sentences per result: what changed, and why.*

## Task 9 · Busy hours of each zone

![Trips per hour for the 20 busiest pickup zones](output/task10_heatmap.png)

*Which zones are busy at night? Which ones in the morning? 3-4 sentences of conclusions.*

## Member contributions

| Member | Name | Tasks | Commits | Pull Requests | What I did |
|---|---|---|---|---|---|
| A | | 1-3 | | | |
| B | | 4-5 | | | |
| C | | 6-7 | | | |
| D | | 8 | | | |
| E | | 9 | | | |
