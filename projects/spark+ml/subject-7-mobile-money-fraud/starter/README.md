# Mobile Money Fraud · Group X

**Big Data with PySpark · group project · 22 November to 19 December 2026 · Subject 7**

> Replace **X** with your group number, in this file and in the file names.

You are the data team of a mobile-money operator: customers pay and send money from their phone. Fraudsters take control of accounts, transfer the money to accounts they hold, then cash it out. The head of security asks:

> *"What does fraud look like in our transactions?"*

You answer with Spark: query 6.36 million transactions with the PySpark DataFrame API. Fraud is a very rare event: fewer than 2 transactions in 1,000 are fraudulent.

This README has two parts. **Part 1** tells you how to start: the team, the data, the setup, the files, the tasks and the rules. **Part 2** is your project report: fill it in as you finish each task. The full instructions are in your teacher's repository: [Overview & rules](https://github.com/HaboussiCodes/BigData_course/tree/main/projects/spark%2Bml) and [Subject 7](https://github.com/HaboussiCodes/BigData_course/tree/main/projects/spark%2Bml/subject-7-mobile-money-fraud).

# Part 1 · Getting started

## Team

| Member | Name | Student ID | GitHub username | Tasks |
|---|---|---|---|---|
| A | | | | 1-3: loading and cleaning, filtering and sorting, aggregation with `groupBy` |
| B | | | | 4-5: joins, Spark SQL |
| C | | | | 6-7: charts, window functions |
| D | | | | 8-9: `spark-submit` script, performance experiment |
| E (G7 only) | | | | 10: extension |

Your teacher gives each member one block of tasks. Each member commits the code of their own block, but everyone must understand the whole project: at the oral check, the teacher can ask any member about any task.

### Week 1 checklist

- [ ] Create the repository **`mobile-money-fraud-group-X`** on GitHub, and add all the members and your teacher (`HaboussiCodes`).
- [ ] Commit this `README.md` and the `.gitignore`.
- [ ] Download the data into `data/` (section 1).
- [ ] Every member installs the software and passes the quick check (section 2).
- [ ] Member A pushes Task 1 first: all the other tasks start from its clean data.

## 1. Get the data

"Synthetic Financial Datasets For Fraud Detection" on Kaggle: [kaggle.com/datasets/ealaxi/paysim1](https://www.kaggle.com/datasets/ealaxi/paysim1). The data comes from PaySim, a simulator built from one month of real logs of a mobile-money service in an African country. Create a free account on [kaggle.com](https://www.kaggle.com), open the link, and click **Download**. The files come as one `.zip`: unzip it into `data/`.

| File | Content | Size |
|---|---|---|
| The CSV file whose name starts with `PS_` | One row per transaction | About 6.36 million transactions over 31 days, of which 8,213 are frauds (0.13%) |

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
mobile-money-fraud-group-X/
├── README.md                         <- this file, completed as your report
├── Mobile_Money_Fraud_GroupX.ipynb   <- Tasks 1-7 and 9 (and 10 for G7)
├── fraud_detection_job.py            <- Task 8
├── output/                           <- charts, logs, screenshots
├── data/                             <- the dataset, never committed
└── .gitignore
```

The notebook has one markdown header per task, with the code, the outputs, the charts and your written analysis. It must run from top to bottom without errors. Write the Parquet copies of Tasks 1 and 9 inside `data/`, never in `output/`.

## 4. How to run

**Notebook:** start `jupyter notebook`, open `Mobile_Money_Fraud_GroupX.ipynb`, then restart the kernel and run all the cells.

**Script (Task 8):** from the repository folder (Git Bash on Windows), save the full terminal output as evidence:

```bash
mkdir -p output/spark_submit
spark-submit --master "local[*]" --driver-memory 4g fraud_detection_job.py data/ 2>&1 | tee output/spark_submit/run.log
```

The script creates its own `SparkSession`, reads the data path from the command line (`sys.argv[1]`), and ends with `spark.stop()`.

## 5. Tasks, owners and deadlines

| Task | What you do | Member | Push by |
|---|---|---|---|
| 1 | Load and inspect: read the CSV file, count the nulls of each column, add `hour` and `day` | A | 28 Nov |
| 2 | Select, filter, sort: the largest transactions, frauds and flagged transactions, payments to merchants | A | 28 Nov |
| 3 | Aggregate: fraud rate per type, the old rule against the real frauds | A | 28 Nov |
| 4 | Joins: a table of type descriptions, transfers followed by cash-outs, accounts that never send (`left_anti`) | B | 5 Dec |
| 5 | Spark SQL: where the fraudulent money goes | B | 5 Dec |
| 6 | Charts: the fraud rate per hour and per day | C | 5 Dec |
| 7 | Window functions: the largest transactions of each type, the running total of fraudulent money | C | 5 Dec |
| 8 | The `spark-submit` script `fraud_detection_job.py` (Tasks 1 and 3) | D | 12 Dec |
| 9 | Performance: `local[1]` against `local[*]`, CSV against Parquet | D | 19 Dec |
| 10 | Extension: fraud patterns by amount | E (G7) | 19 Dec |

**Points (out of 100, 110 for G7):** Tasks 1-7: 65 · Tasks 8-9: 20 · README report: 10 · Git workflow: 5 · Task 10 (G7 only): 10 · bonus, cluster execution: +5. Each member's grade out of 20 is the group score × their contribution share ÷ 5 (÷ 5.5 for G7).

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

### Task 1 · Load and inspect

| Question | Answer |
|---|---|
| Rows | |
| Nulls per column | |

### Task 2 · Select, filter and sort

*The 10 largest transactions: `step`, `type`, `amount`, `nameOrig`, `nameDest`, `isFraud`.*

| Question | Answer |
|---|---|
| Number of frauds | |
| Transactions flagged by the old rule (`isFlaggedFraud = 1`) | |
| Types of the transactions that go to a merchant | |

### Task 3 · Aggregate with groupBy

*The table per type: number of transactions, average amount, number of frauds, fraud rate.*

| Question | Answer |
|---|---|
| Which types carry fraud? | |
| Frauds caught / missed by the old rule | |

### Task 4 · Join tables

*Your table of type descriptions, joined with the type table of Task 3.*

| Question | Answer |
|---|---|
| Destinations of a fraudulent transfer that then make a cash-out | |
| What does it tell you about the simulator? | |
| Destination accounts that never send money (`left_anti`) | |

### Task 5 · Where does the money go? (Spark SQL)

*Your SQL query, and its result: the 10 destination accounts that received the most fraudulent money, with their incoming transactions, fraudulent and in total.*

### Task 6 · When does fraud happen? (charts)

![Fraud rate per hour of the day](output/task6_fraud_rate_per_hour.png)
![Fraud rate per day](output/task6_fraud_rate_per_day.png)

*3-5 sentences that explain the two charts. At which hours and on which days is the fraud rate highest?*

### Task 7 · Window functions

*The 3 largest transactions of each type, with `isFraud`.*

| Question | Answer |
|---|---|
| Are the largest transactions frauds? | |
| Largest amount of a fraud | |
| Day when the running total passes half of all the fraudulent money | |

## Phase B: execution and performance (Tasks 8-9)

### Task 8 · Standalone script

| Item | Answer |
|---|---|
| Command used | `spark-submit --master "local[*]" --driver-memory 4g fraud_detection_job.py data/` |
| Log | [`output/spark_submit/run.log`](output/spark_submit/run.log) |
| Tables written in `output/` | *the CSV folders of Task 3* |

### Task 9 · Performance experiment

*Run time of Task 3 on the full data. Run each setting 3 times and give the average. Time an action (`count()`, `collect()` or a write), not a transformation.*

| Setting | Run 1 (s) | Run 2 (s) | Run 3 (s) | Average (s) |
|---|---|---|---|---|
| `local[1]`, CSV | | | | |
| `local[*]`, CSV | | | | |
| `local[*]`, Parquet copy | | | | |

![Spark UI: the stages of one job](output/spark_ui.png)

*One or two sentences per result: what changed, and why.*

### Bonus (+5 points, optional) · Cluster execution

*If you ran the script on a real cluster (YARN or Spark standalone) or on a cloud notebook service, with the data on HDFS or cloud storage: give the command and the log.*

## Task 10 (G7 only) · Fraud patterns by amount

*The table of the number of transactions, the number of frauds and the fraud rate per amount band and type (`TRANSFER`, `CASH_OUT`).*

![Fraud rate per amount band](output/task10_fraud_rate_per_band.png)

| Question | Answer |
|---|---|
| Frauds whose amount is the whole balance of the origin account | |
| Honest transactions that do the same | |

*3-4 sentences: what do fraudsters do with the accounts they take over?*

## Member contributions

| Member | Name | Tasks | Commits | Pull Requests | What I did |
|---|---|---|---|---|---|
| A | | 1-3 | | | |
| B | | 4-5 | | | |
| C | | 6-7 | | | |
| D | | 8-9 | | | |
| E (G7 only) | | 10 | | | |
