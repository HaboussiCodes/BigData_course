# Rain Tomorrow · Group X

**Big Data with PySpark · group project ·**

> Replace **X** with your group number, in this file and in the file names.

You are the data team of a weather service. The director asks:

> *"How do climates differ around the world and across Algeria?"*

You answer with Spark: query two years of daily observations from thousands of weather stations with the PySpark DataFrame API.

This README has two parts. **Part 1** tells you how to start: the team, the data, the setup, the files, the tasks and the rules. **Part 2** is your project report: fill it in as you finish each task. The full instructions are in your teacher's repository: [Overview & rules](https://github.com/HaboussiCodes/BigData_course/tree/main/projects/spark%2Bml) and [Subject 6](https://github.com/HaboussiCodes/BigData_course/tree/main/projects/spark%2Bml/subject-6-rain-tomorrow).

# Part 1 · Getting started

## Team

| Member | Name | Student ID | GitHub username | Tasks |
|---|---|---|---|---|
| A | | | | 1-3: loading and cleaning, filtering and sorting, aggregation with `groupBy` |
| B | | | | 4-5: joins, Spark SQL |
| C | | | | 6-7: charts, window functions |
| D | | | | 8: performance experiment |
| E | | | | 9: |

Your teacher gives each member one block of tasks. Each member commits the code of their own block, but everyone must understand the whole project: at the oral check, the teacher can ask any member about any task.

### Week 1 checklist

- [ ] Create the repository **`rain-tomorrow-group-X`** on GitHub, and add all the members and your teacher (`HaboussiCodes`).
- [ ] Commit this `README.md` and the `.gitignore`.
- [ ] Download the data into `data/` (section 1).
- [ ] Every member installs the software and passes the quick check (section 2).
- [ ] Member A pushes Task 1 first: all the other tasks start from its clean data.

## 1. Get the data

The Global Surface Summary of the Day (GSOD) of NOAA, from the [NCEI archive](https://www.ncei.noaa.gov/data/global-summary-of-the-day/archive/): one `.tar.gz` per year, holding one CSV file per station. Also download the station list. No account is needed. Run these commands from the repository folder; on Windows, run them in Git Bash, which comes with Git and includes `curl` and `tar`:

```bash
mkdir -p data/gsod && cd data/gsod
for y in 2023 2024; do
  curl -O https://www.ncei.noaa.gov/data/global-summary-of-the-day/archive/$y.tar.gz
  mkdir -p $y && tar -xzf $y.tar.gz -C $y
done
curl -O https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv
cd ../..
```

| File | Content |
|---|---|
| `data/gsod/2023/*.csv`, `data/gsod/2024/*.csv` | One file per station and year, one row per day: millions of rows per year |
| `data/gsod/isd-history.csv` | One row per station: `USAF`, `WBAN`, `STATION NAME`, `CTRY` (country code), `LAT`, `LON`, `ELEV(M)` |

The [GSOD readme](https://git.lsit.ucsb.edu/publicdata/NCEI-NOAA/src/branch/main/global-summary-of-the-day/doc/readme.txt) describes all the columns.

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
rain-tomorrow-group-X/
├── README.md                    <- this file, completed as your report
├── Rain_Tomorrow_GroupX.ipynb   <- Tasks 1-9
├── output/                      <- charts, logs, screenshots
├── data/                        <- the dataset, never committed
└── .gitignore
```

The notebook has one markdown header per task, with the code, the outputs, the charts and your written analysis. It must run from top to bottom without errors. Write the Parquet copies of Tasks 1 and 9 inside `data/`, never in `output/`.

## 4. How to run

**Notebook:** start `jupyter notebook`, open `Rain_Tomorrow_GroupX.ipynb`, then restart the kernel and run all the cells.

**Script (Task 8):** from the repository folder (Git Bash on Windows), save the full terminal output as evidence:

```bash
mkdir -p output/spark_submit
spark-submit --master "local[*]" --driver-memory 4g rain_tomorrow_job.py data/ 2>&1 | tee output/spark_submit/run.log
```

The script creates its own `SparkSession`, reads the data path from the command line (`sys.argv[1]`), and ends with `spark.stop()`.

## 5. Tasks, owners and deadlines

| Task | What you do | Member | Push by |
|---|---|---|---|
| 1 | Load and clean: read all the station files as strings, replace the missing-value codes, convert to °C, add `rain`, save a Parquet copy | A | 28 Nov |
| 2 | Select, filter, sort: the hottest days, days above 45 °C, the Algiers stations | A | 28 Nov |
| 3 | Aggregate: days, temperature and rain per station, stations and rows per year | A | 28 Nov |
| 4 | Joins: countries from `isd-history.csv`, the rainiest and driest countries, stations missing from the list (`left_anti`) | B | 5 Dec |
| 5 | Spark SQL: Algeria from north to south | B | 5 Dec |
| 6 | Charts: the seasons in Algiers | C | 5 Dec |
| 7 | Window functions: the 3 hottest days of each Algerian station, daily temperature drops in Algiers | C | 5 Dec |
| 8 | The `spark-submit` script `rain_tomorrow_job.py` (Tasks 1 and 3) | D | 12 Dec |
| 9 | Performance: `local[1]` against `local[*]`, small CSV files against Parquet | D | 19 Dec |
| 10 | Extension: a climate map of the world | E (G7) | 19 Dec |

**Points (out of 100, 110 for G7):** Tasks 1-7: 65 · Tasks 8-9: 20 · README report: 10 · Git workflow: 5 · Task 10 (G7 only): 10 · bonus, cluster execution: +5. Each member's grade out of 20 is the group score × their contribution share ÷ 5 (÷ 5.5 for G7).

## 6. Rules

- **Branches and Pull Requests:** one branch per task (`git checkout -b task6-yourname`), small commits as you progress, then a Pull Request to `main`, reviewed and merged by another member. One large commit at the end does not count.
- **Your name on your code:** every notebook cell and every script section starts with a header. If your name appears on no code, you are treated as a non-contributor.

  ```python
  # ============================================
  # Task 3: Aggregation with groupBy
  # Author: First Last name
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
| Rows | |
| Stations | |
| Nulls per column (the columns with nulls) | |

### Task 2 · Select, filter and sort

*The 10 hottest days in the data: station `NAME`, `DATE` and `MAX` in °C. Are they plausible?*

| Question | Answer |
|---|---|
| Station-days with a maximum above 45 °C | |
| Rainy days in the whole data | |
| Algiers stations (`STATION` code and coordinates) | |

### Task 3 · Aggregate with groupBy

| Question | Answer |
|---|---|
| Stations with fewer than 300 days of data | |

*The table per year: number of stations and number of rows.*

### Task 4 · Join tables

*The 10 rainiest and the 10 driest countries (at least 10 stations): number of stations, average temperature, share of rainy days.*

| Question | Answer |
|---|---|
| Where does Algeria (`CTRY = "AG"`) stand? | |
| Stations of the observations missing from `isd-history.csv` (`left_anti`) | |

### Task 5 · Algeria from north to south (Spark SQL)

*Your SQL query, and its result: the Algerian stations with at least 300 days of data, from north to south: name, latitude, number of days, average temperature, number of rainy days.*

### Task 6 · Seasons in Algiers (charts)

![Average temperature per month in Algiers](output/task6_algiers_temperature.png)
![Share of rainy days per month in Algiers](output/task6_algiers_rain.png)

*3-5 sentences that explain the two charts. When is the rainy season?*

### Task 7 · Window functions

*The 3 hottest days of each Algerian station, and the 5 largest drops of temperature from one day to the next in Algiers.*

*In which season are the largest drops?*

## Phase B: execution and performance

### Task 8 · Performance experiment

*Run time of Task 3 on the full data. Run each setting 3 times and give the average. Time an action (`count()`, `collect()` or a write), not a transformation.*

| Setting | Partitions | Run 1 (s) | Run 2 (s) | Run 3 (s) | Average (s) |
|---|---|---|---|---|---|
| `local[1]`, CSV files | | | | | |
| `local[*]`, CSV files | | | | | |
| `local[*]`, Parquet copy | | | | | |

*One or two sentences per result: what changed, and why.*

*Why are thousands of small files slow to read?*

## Task 9· Climate map

![Stations coloured by average temperature](output/task10_map_temperature.png)
![Stations coloured by the share of rainy days](output/task10_map_rain.png)

*Where are the rainiest regions? Where does Algeria stand? 3-4 sentences of conclusions.*

## Member contributions

| Member | Name | Tasks | Commits | Pull Requests | What I did |
|---|---|---|---|---|---|
| A | | 1-3 | | | |
| B | | 4-5 | | | |
| C | | 6-7 | | | |
| D | | 8 | | | |
| E  | | 9 | | | |
