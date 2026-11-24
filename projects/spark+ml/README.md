# Big Data Group Projects with PySpark

 Students work in  groups. Each group gets a different subject built on a real public dataset of several million rows. All subjects follow the same two phases, the same deliverables, so every group does comparable work.

## The subjects

Each subject uses a different real dataset, with the same nine tasks and the same grid: seven PySpark query tasks, the performance experiment. Open a subject to read its full description.

| # | Subject | Domain | Dataset | Size | Data access |
| --- | --- | --- | --- | --- | --- |
| 1 | [1 · Flight delays](subject-1-flight-delay/) | Air transport | US DOT flights, 2015 | 5.8 M flights | Kaggle account | 
| 2 | [2 · NYC taxi trip time](subject-2-nyc-taxi/) | City transport | NYC yellow taxi trips, January-March 2024 | About 9 M trips | Free download |
| 3 | [3 · Instacart segments](subject-3-instacart/) | Retail | Instacart orders, 2017 | 3.4 M orders, 33.8 M basket lines | Kaggle account |
| 4 | [4 · Book recommender](subject-4-book-recommender/) | Books | Goodbooks-10k (Goodreads) | 6 M ratings | Free download (GitHub) |
| 5 | [5 · Amazon review sentiment](subject-5-amazon-reviews/) | Online shopping, text | Amazon Reviews 2023, Video Games | 4.6 M reviews | Free download |
| 6 | [6 · Rain tomorrow](subject-6-rain-tomorrow/) | Climate | NOAA daily weather, 2023-2024 | Millions of station-days | Free download |
| 7 | [7 · Mobile money fraud](subject-7-mobile-money-fraud/) | Finance | PaySim mobile money | 6.36 M transactions | Kaggle account |

Each subject folder holds the full subject, and a `starter/` folder with the two files to copy into your group's repository: its `README.md` (your report) and its `.gitignore`.

## Subjects

The 7 subjects are: 1 Flight delays, 2 NYC taxi trip time, 3 Instacart customer segments, 4 Book recommender, 5 Amazon review sentiment, 6 Rain tomorrow, 7 Mobile money fraud. Each one is described in full in its folder: see [The 7 subjects](#the-7-subjects) above.

| Group | Subject | Member A (Tasks 1-3) | Member B (Tasks 4-5) | Member C (Tasks 6-7) | Member D (Tasks 8) | Member E (Task 9) | GitHub repository |
| --- | --- | --- | --- | --- | --- | --- | --- |
| G1 |  |  |  |  |  |  |  |
| G2 |  |  |  |  |  |  |  |
| G3 |  |  |  |  |  |  |  |
| G4 |  |  |  |  |  |  |  |
| G5 |  |  |  |  |  |  |  |
| G6 |  |  |  |  |  |  |  |
| G7 |  |  |  |  |  |  |  |

## Calendar

The project runs over 2 weeks, with a checkpoint at the end of each week and the final submission on 2026-12-14 at 23:59. At each checkpoint the teacher looks at your repository. Checkpoints have no points of their own, but they count in the Git workflow score and in each member's contribution level.

| Week | Dates | Work to finish | Pushed to GitHub by |
| --- | --- | --- | --- |
| 1 | - | Form the group, install Spark, create the repository, download the data, Tasks 1-3 | - |
| 2 | - | Tasks 4-7: Phase A complete | - |
| 3 | - | Task 8: performance | - |
| 4 | - | Task 9 , README report | -|
| Oral |  after the deadline | 10 minutes per group: 5-minute demo of the results, then each member explains a part of their own code | 16 December 2026 |

## The two phases

Every subject has the same 9 tasks in two phases, plus Task 10 for the group of 5. The project is mostly about PySpark queries: Tasks 1-7 use the DataFrame API (`select`, `filter`, `groupBy`, `join`, `Window`), except Task 5, which uses Spark SQL. Tasks 1-7 and 10 depend on the subject and are described in its folder. Tasks 8-9 are the same for every group and are described here.

| Phase | Tasks |What you do |
| --- | --- | --- |
| A. PySpark queries | 1-7 | Load and clean the data, filter and sort, aggregate with `groupBy`, join tables, write one Spark SQL query, draw two charts of patterns over time, rank with window functions |
| B. Execution and performance | 8 | Measure how Spark settings change the run time |



### Task 8: Performance experiment

Measure the run time of your Task 3 on the full dataset under these settings. Run each setting 3 times and report the average. Spark is lazy: time an action such as `count()`, `collect()` or a write, not a transformation.

| Experiment | Settings to compare |
| --- | --- |
| Parallelism | `local[1]`, then `local[*]` |
| File format | The data in its original format, then the same data saved and re-read as Parquet. If your data already comes as Parquet, compare it with a CSV copy. |

**Deliverable:** one results table, a screenshot of the Spark UI (`localhost:4040`) showing the stages of one job, and a short explanation of each result.

## Work distribution

The teacher gives each member one block of tasks, written in the table of the Groups and subjects section. Each member commits the code for their own block. Everyone must still understand the whole project: at the oral check, the teacher can ask any member about any task.

| Member | Tasks | Phase |
| --- | --- | --- |
| A | Tasks 1-3: loading and cleaning, filtering and sorting, aggregation with `groupBy` |- |
| B | Tasks 4-5: joins, Spark SQL | - |
| C | Tasks 6-7: charts of patterns over time, window functions |- |
| D | Tasks 8: performance experiment | - |
| E  | Task 9 | -|

Task 1 comes first because every other task uses the cleaned data. The README is written together: each member writes the part about their own tasks.

### Put your name on your code

Every notebook cell or script section starts with a header naming its author:

```python
# ============================================
# Task 3: Aggregation with groupBy
# Author: First Last name
# ============================================
```

If your name appears on no code, you are treated as a non-contributor.

## Git workflow and deliverables

Each group submits one GitHub repository. Its name is given in your subject page, for example `flight-delay-group-3`. Add the teacher (`HaboussiCodes`) as a collaborator .

### Git workflow

1. Create a branch for your task: `git checkout -b task6-yourname`
2. Commit your work in small steps as you progress.
3. Push the branch and open a Pull Request to `main`.
4. Another member reviews and merges it.

Your GitHub username must appear in the commit history with meaningful code changes. One large commit at the end does not count.

### Deliverables

1. **Notebook** `<Subject>_GroupX.ipynb`: Tasks 1-7, 9  with code, outputs, charts and written analysis, one markdown header per task. It must run from top to bottom without errors.
3. **README.md**, the project report:
   - Team members (names and student IDs)
   - Executive summary (2-3 sentences)
   - Main findings of Phase A, with the charts
   - Performance experiment table and conclusions
   - Member contributions table

```text
subject-name-group-X/
├── README.md
├── Subject_GroupX.ipynb
├── subject_job.py
├── output/          <- charts, logs, screenshots
├── data/            <- raw data, never committed (.gitignore)
└── .gitignore
```

Do not commit the data. Add `data/` to `.gitignore`: GitHub refuses files larger than 100 MB.

## Evaluation

The group earns a score out of 15.

### Group score

| Component | Points | Details |
| --- | --- | --- |
| Phase A: PySpark queries (Tasks 1-7) | 12 | Correct results, DataFrame API used where asked, Spark SQL in Task 5, window functions, interpretation |
| Phase B: Execution and performance (Tasks 8) | 1 | measured and explained results |
| README report | 1 | All sections present, clear conclusions |
| Git workflow | 1 | Branches and Pull Requests used |
| **Total** | **15** |  |
### Individual adjustment

| Contribution level | Share of group score |
| --- | --- |
| Strong: regular commits, Pull Requests, code authorship | 100% |
| Moderate: some commits, took part in reviews | 70-90% |
| Minimal: few or trivial commits | 30-60% |
| No commits | 0% |

During the oral check, each student explains a part of their own code chosen by the teacher. A student who cannot explain their code receives zero.

## Setup

The whole project runs on a laptop in `local[*]` mode: no cluster is needed. All groups use the same versions, so results and errors can be compared. This combination was tested on 20 November 2026.

| Software | Version | Install |
| --- | --- | --- |
| Java (JDK) | 17, not 21 or later | adoptium.net, Temurin 17 LTS |
| Python | 3.11 | python.org/downloads |
| PySpark | 3.5.x | `pip install "pyspark==3.5.*"` |
| pandas, matplotlib, pyarrow, Jupyter | pandas below 3 | `pip install "pandas<3" matplotlib pyarrow jupyter` |

**Quick check:**

```bash
java -version      # must print 17.x
python --version   # 3.11.x
python -c "from pyspark.sql import SparkSession; s = SparkSession.builder.master('local[*]').getOrCreate(); print('Spark', s.version, 'works'); s.stop()"
```

**First cell of every notebook:**

```python
import os, sys
os.environ["PYSPARK_PYTHON"] = sys.executable   # avoids worker errors on Windows
from pyspark.sql import SparkSession, functions as F
spark = (SparkSession.builder.master("local[*]").appName("project")
         .config("spark.driver.memory", "4g")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
```

**Common setup issues:**

| Problem | Solution |
| --- | --- |
| `java: command not found` or `JAVA_HOME is not set` | Install JDK 17 and set `JAVA_HOME` to its folder |
| `Py4JJavaError` when Spark starts, or `sun.misc.Unsafe ... not available` in `toPandas()` | Wrong Java version: PySpark 3.5 needs Java 17 (or 11), not 21 or later |
| Windows: `HADOOP_HOME and hadoop.home.dir are unset`, or `UnsatisfiedLinkError` when writing a file | Copy `winutils.exe` and `hadoop.dll` from a `hadoop-3.3.x/bin` folder of [github.com/cdarlint/winutils](https://github.com/cdarlint/winutils) into `C:\hadoop\bin`, set `HADOOP_HOME=C:\hadoop`, add `%HADOOP_HOME%\bin` to `PATH`, open a new terminal |
| Windows: `Python worker failed to connect back` | Set `PYSPARK_PYTHON` as in the first cell above |
| `FutureWarning` about pandas 3, or errors in `toPandas()` | `pip install "pandas<3"` |
| Logs flood the terminal | `spark.sparkContext.setLogLevel("WARN")` |
| `OutOfMemoryError` | Set `spark.driver.memory` to at most half of your RAM, and work on a sample |

**Google Colab** is accepted for the notebook if you cannot install Java. Start with `!pip install -q "pyspark==3.5.*"` and run `!java -version`. If it shows 21 or later, install Java 17 with `!apt-get install -y -qq openjdk-17-jdk-headless` and set `os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"` before starting Spark. Colab deletes your files when the session ends, so keep the data in Google Drive. 

## Datasets

Each subject uses one public dataset. Download it in week 1, put it in the `data/` folder of your repository, and never commit it. The exact download steps are in your subject page; this table gathers them.

| # | Subject | Dataset | Files to download | Account | Link |
| --- | --- | --- | --- | --- | --- |
| 1 | Flight delays | 2015 Flight Delays and Cancellations (US DOT), 5.8 million flights | `flights.csv`, `airlines.csv`, `airports.csv` | Kaggle | [kaggle.com/datasets/usdot/flight-delays](https://www.kaggle.com/datasets/usdot/flight-delays) |
| 2 | NYC taxi trip time | NYC yellow taxi trips, January to March 2024, about 9 million trips | `yellow_tripdata_2024-01.parquet` to `-03`, `taxi_zone_lookup.csv` | None | [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) |
| 3 | Instacart segments | Instacart Online Grocery Shopping 2017, 3.4 million orders | The six CSV files | Kaggle, and accept the competition rules once | [Instacart Market Basket Analysis](https://www.kaggle.com/c/instacart-market-basket-analysis/data) |
| 4 | Book recommender | goodbooks-10k (Goodreads), 6 million ratings | `ratings.csv`, `books.csv`, `to_read.csv`, `book_tags.csv`, `tags.csv` | None | [github.com/zygmuntz/goodbooks-10k](https://github.com/zygmuntz/goodbooks-10k) |
| 5 | Amazon review sentiment | Amazon Reviews 2023, Video Games category, 4.6 million reviews | `Video_Games.jsonl.gz`, `meta_Video_Games.jsonl.gz` | None | [Amazon Reviews 2023](https://amazon-reviews-2023.github.io) |
| 6 | Rain tomorrow | NOAA Global Surface Summary of the Day, 2023 and 2024 | `2023.tar.gz`, `2024.tar.gz`, `isd-history.csv` | None | [NCEI GSOD archive](https://www.ncei.noaa.gov/data/global-summary-of-the-day/archive/) |
| 7 | Mobile money fraud | PaySim synthetic mobile-money transactions, 6.36 million rows | The CSV file whose name starts with `PS_` | Kaggle | [kaggle.com/datasets/ealaxi/paysim1](https://www.kaggle.com/datasets/ealaxi/paysim1) |

- **Kaggle (Subjects 1, 3 and 7):** create a free account on kaggle.com, open the link, and click **Download**. The files come as one `.zip`: unzip it into `data/`.
- **No account (Subjects 2, 4, 5 and 6):** the `curl` commands of your subject page download everything. On Windows, run them in Git Bash.
- **Disk space:** keep about 5 GB free, for the raw files and the Parquet copies of Tasks 1 and 9.
- **A slow or blocked connection:** one member downloads the files once and shares them inside the group (a USB stick or a shared drive), never through GitHub.

## AI usage policy

- **Allowed:** debugging errors, conceptual explanations, generating comments.
- **Prohibited:** generating entire tasks or the whole pipeline.
- **Penalty:** a student who cannot explain their code during the oral check receives zero.
