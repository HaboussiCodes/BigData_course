# Flight Delay Analytics · Group X

**Big Data with PySpark · group project · 22 November to 19 December 2026 · Subject 1**

> Replace **X** with your group number, in this file and in the file names.

You are the data team of an airline operations centre. The Operations Director has one year of flight records, about 5.8 million flights, and asks:

> *"Where and when do our delays happen?"*

You answer it with Apache Spark: query the data with the PySpark DataFrame API.

This README has two parts. **Part 1** tells you how to start: the team, the data, the setup, the files, the tasks and the rules. **Part 2** is your project report: fill it in as you finish each task. The full instructions are in your teacher's repository: [Overview & rules](https://github.com/HaboussiCodes/BigData_course/tree/main/projects/spark%2Bml) and [Subject 1](https://github.com/HaboussiCodes/BigData_course/tree/main/projects/spark%2Bml/subject-1-flight-delay).

# Part 1 · Getting started

## Team

| Member | Name | Student ID | GitHub username | Tasks |
|---|---|---|---|---|
| A | | | | 1-3: loading and cleaning, filtering and sorting, aggregation with `groupBy` |
| B | | | | 4-5: joins, Spark SQL |
| C | | | | 6-7: charts, window functions |
| D | | | | 8: performance experiment |
| E | | | | 9:the causes of delays per airline and per airport|

### Week 1 checklist

- [ ] Create the repository **`flight-delay-group-X`** on GitHub, and add all the members and your teacher (`HaboussiCodes`).
- [ ] Commit this `README.md` and the `.gitignore`.
- [ ] Download the data into `data/` (section 1).
- [ ] Every member installs the software and passes the quick check (section 2).
- [ ] Member A pushes Task 1 first: all the other tasks start from its clean data.

## 1. Get the data

The "2015 Flight Delays and Cancellations" dataset of the US Department of Transportation, on Kaggle: [kaggle.com/datasets/usdot/flight-delays](https://www.kaggle.com/datasets/usdot/flight-delays). Create a free account on [kaggle.com](https://www.kaggle.com), open the link, and click **Download**. The files come as one `.zip`: unzip it into `data/`.

| File | Content | Size |
|---|---|---|
| `flights.csv` | One row per flight in 2015 | About 5.8 million rows |
| `airlines.csv` | Airline code and name | 14 rows |
| `airports.csv` | Airport code, name, city, state | About 320 rows |

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
flight-delay-group-X/
├── README.md                   <- this file, completed as your report
├── Flight_Delay_GroupX.ipynb   <- Tasks 1-7 and 9 (and 10 for G7)
├── flight_delay_job.py         <- Task 8
├── output/                     <- charts, logs, screenshots
├── data/                       <- the dataset, never committed
└── .gitignore
```

The notebook has one markdown header per task, with the code, the outputs, the charts and your written analysis. It must run from top to bottom without errors. Write the Parquet copies of Tasks 1 and 9 inside `data/`, never in `output/`.

## 4. How to run

**Notebook:** start `jupyter notebook`, open `Flight_Delay_GroupX.ipynb`, then restart the kernel and run all the cells.

## 5. Tasks, owners and deadlines

| Task | What you do | Member | Push by |
|---|---|---|---|
| 1 | Load and clean: read `flights.csv`, explain the nulls of `ARRIVAL_DELAY`, remove the cancelled and diverted flights, add `DEP_HOUR` and `label` | - | - |
| 2 | Select, filter, sort: the longest delays, Atlanta in July, the number of origin airports | - | / |
| 3 | Aggregate: the punctuality of each airline, the 10 busiest airports | - | - |
| 4 | Joins: airline names, the delay share of each state, unknown airport codes (`left_anti`) | - | / |
| 5 | Spark SQL: the 10 airports with the highest delay rate | -| - |
| 6 | Charts: the delay rate per month and per departure hour | - | - |
| 7 | Window functions: the worst routes of each airline, the monthly change of the delay rate | - | - |
| 8 | Performance: `local[1]` against `local[*]`, CSV against Parquet | - | - |
| 9 |  the causes of delays per airline and per airport | - |- |



## 6. Rules

- **Branches and Pull Requests:** one branch per task (`git checkout -b task6-yourname`), small commits as you progress, then a Pull Request to `main`, reviewed and merged by another member. One large commit at the end does not count.
- **Your name on your code:** every notebook cell and every script section starts with a header. If your name appears on no code, you are treated as a non-contributor.

  ```python
  # ============================================
  # Task 3: Aggregation with groupBy
  # Author: First Last name
  # ============================================
  ```

- **Teacher access:** add your teacher (`HaboussiCodes`) as a collaborator of the repository.
- **Data:** never commit the data, the Parquet copies or any large file. GitHub refuses files larger than 100 MB.
- **AI tools:** allowed for debugging errors, conceptual explanations and comments. Generating a whole task or the whole pipeline is not allowed. At the oral check, a student who cannot explain their own code gets zero.
- **Oral check:** after the deadline, 10 minutes per group: a 5-minute demo of the results, then each member explains a part of their own code.

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
| Nulls in `ARRIVAL_DELAY` | |
| Cancelled flights / diverted flights | |
| Why do these flights have no arrival delay? | |

### Task 2 · Select, filter and sort

*The 10 flights with the longest arrival delay (paste the table).*

| Question | Answer |
|---|---|
| Flights that left Atlanta (`ATL`) in July / of which delayed | |
| Number of different origin airports | |
| Why is it larger than the number of rows of `airports.csv`? | |

### Task 3 · Aggregate with groupBy

*The airline table, from the most to the least punctual, and the 10 busiest origin airports (paste the tables).*

*2-3 sentences: which airlines and airports stand out?*

### Task 4 · Join tables

*The airline table with the names, and the 5 states with the highest share of delayed flights (at least 10,000 flights).*

| Question | Answer |
|---|---|
| Origin codes of `flights.csv` missing from `airports.csv` (`left_anti`) | |
| In which month are they? | |

### Task 5 · Airports with the most delays (Spark SQL)

*Your SQL query, and its result: the 10 origin airports with the highest delay rate (at least 10,000 departures), with name and city.*

### Task 6 · When do delays happen? (charts)

![Delay rate per month](output/task6_delay_rate_per_month.png)
![Delay rate per departure hour](output/task6_delay_rate_per_hour.png)

*3-5 sentences that explain the two charts.*

### Task 7 · Window functions

*The 3 routes with the highest average arrival delay for each airline (routes with at least 500 flights), and the 5 largest monthly increases of the delay rate.*

*2-3 sentences of interpretation.*

## Phase B: execution and performance (Tasks 8)


### Task 8 · Performance experiment

*Run time of Task 3 on the full data. Run each setting 3 times and give the average. Time an action (`count()`, `collect()` or a write), not a transformation.*

| Setting | Run 1 (s) | Run 2 (s) | Run 3 (s) | Average (s) |
|---|---|---|---|---|
| `local[1]`, CSV | | | | |
| `local[*]`, CSV | | | | |
| `local[*]`, Parquet copy | | | | |

![Spark UI: the stages of one job](output/spark_ui.png)

*One or two sentences per result: what changed, and why.*

## Task 9 · Causes of delays

![Share of delay minutes per cause and per airline](output/task10_causes_per_airline.png)

*The table of the cause shares for the 20 busiest origin airports. Which airports suffer most from weather?*

*3-4 sentences of conclusions.*

## Member contributions

| Member | Name | Tasks | Commits | Pull Requests | What I did |
|---|---|---|---|---|---|
| A | | 1-3 | | | |
| B | | 4-5 | | | |
| C | | 6-7 | | | |
| D | | 8 | | | |
| E | | 9 | | | |
