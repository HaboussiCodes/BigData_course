# Amazon Review Sentiment · Group X

**Big Data with PySpark · group project . Subject 5**

> Replace **X** with your group number, in this file and in the file names.

You are the data team of an online video-game shop. Customers write millions of reviews, and the customer-care manager wants to find unhappy customers fast:

> *"What do customers say about our games and consoles?"*

You answer with Spark: query 4.6 million reviews with the PySpark DataFrame API.

This README has two parts. **Part 1** tells you how to start: the team, the data, the setup, the files, the tasks and the rules. **Part 2** is your project report: fill it in as you finish each task. The full instructions are in your teacher's repository: [Overview & rules](https://github.com/HaboussiCodes/BigData_course/tree/main/projects/spark%2Bml) and [Subject 5](https://github.com/HaboussiCodes/BigData_course/tree/main/projects/spark%2Bml/subject-5-amazon-reviews).

# Part 1 · Getting started

## Team

| Member | Name | Student ID | GitHub username | Tasks |
|---|---|---|---|---|
| A | | | | 1-3: loading and cleaning, filtering and sorting, aggregation with `groupBy` |
| B | | | | 4-5: joins, Spark SQL |
| C | | | | 6-7: charts, window functions |
| D | | | | 8: performance experiment |
| E  | | | | 9|

Your teacher gives each member one block of tasks. Each member commits the code of their own block, but everyone must understand the whole project: at the oral check, the teacher can ask any member about any task.

### Week 1 checklist

- [ ] Create the repository **`amazon-reviews-group-X`** on GitHub, and add all the members and your teacher (`HaboussiCodes`).
- [ ] Commit this `README.md` and the `.gitignore`.
- [ ] Download the data into `data/` (section 1).
- [ ] Every member installs the software and passes the quick check (section 2).
- [ ] Member A pushes Task 1 first: all the other tasks start from its clean data.

## 1. Get the data

The Video_Games category of the [Amazon Reviews 2023](https://amazon-reviews-2023.github.io) dataset (McAuley Lab, UC San Diego). No account is needed. **Do not unzip the files:** Spark reads `.gz` directly. Run these commands from the repository folder; on Windows, run them in Git Bash, which comes with Git:

```bash
mkdir -p data && cd data
curl -O https://datarepo.eng.ucsd.edu/mcauley_group/data/amazon_2023/raw/review_categories/Video_Games.jsonl.gz
curl -O https://datarepo.eng.ucsd.edu/mcauley_group/data/amazon_2023/raw/meta_categories/meta_Video_Games.jsonl.gz
cd ..
```

Each line of these files is one JSON record:

| File | Content | Size |
|---|---|---|
| `Video_Games.jsonl.gz` | One review per line | 4.6 million reviews by 2.8 million users |
| `meta_Video_Games.jsonl.gz` | One product per line | 137,200 products |

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
amazon-reviews-group-X/
├── README.md                     <- this file, completed as your report
├── Amazon_Reviews_GroupX.ipynb   <- Tasks 
├── output/                       <- charts, logs, screenshots
├── data/                         <- the dataset, never committed
└── .gitignore
```

The notebook has one markdown header per task, with the code, the outputs, the charts and your written analysis. It must run from top to bottom without errors. Write the Parquet copies of Tasks 1 and 9 inside `data/`, never in `output/`.

## 4. How to run

**Notebook:** start `jupyter notebook`, open `Amazon_Reviews_GroupX.ipynb`, then restart the kernel and run all the cells.

**Script (Task 8):** from the repository folder (Git Bash on Windows), save the full terminal output as evidence:

```bash
mkdir -p output/spark_submit
spark-submit --master "local[*]" --driver-memory 4g review_sentiment_job.py data/ 2>&1 | tee output/spark_submit/run.log
```

The script creates its own `SparkSession`, reads the data path from the command line (`sys.argv[1]`), and ends with `spark.stop()`.

## 5. Tasks, owners and deadlines

| Task | What you do | Member | Push by |
|---|---|---|---|
| 1 | Load and clean: read the reviews, add `date` and `year`, remove empty texts, save a Parquet copy | - | - |
| 2 | Select, filter, sort: the most helpful reviews, verified purchases, reviews about refunds | -| - |
| 3 | Aggregate: reviews per star, text length per star, verified against non-verified | - | - |
| 4 | Joins: the most reviewed products, reviews without metadata (`left_anti`), rating per price band | - |- |
| 5 | Spark SQL: the 10 stores with the most unhappy customers | - |- |
| 6 | Charts: reviews per year, average rating per year | - | - |
| 7 | Window functions: the top 3 products of each year, the yearly growth of reviews |-| - |
| 8 | Performance: `local[1]` against `local[*]`, `.jsonl.gz` against Parquet | - | - |
| 9 | Extension: the words of unhappy customers | - | - |

**Points (out of 15):** 

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

> Fill in every section before the deadline. Replace the lines in *italics* and fill the empty cells. The numbers must be the same as in your notebook outputs. Save your charts in `output/`: the image links below already point there, so keep the same file names or change the links.

## Executive summary

*2-3 sentences that answer the question of the scenario.*

## Phase A: main findings (Tasks 1-7)

### Task 1 · Load and clean

| Question | Answer |
|---|---|
| Rows before cleaning | |
| Rows after cleaning | |
| First date / last date | |

### Task 2 · Select, filter and sort

*The 10 reviews with the most helpful votes: rating, title, year and number of votes.*

| Question | Answer |
|---|---|
| Reviews from verified purchases (number and share) | |
| Reviews whose text contains "refund" / of which 1 or 2 stars | |

### Task 3 · Aggregate with groupBy

*The number and the share of reviews for each star rating, and the average text length per star rating.*

| Question | Answer |
|---|---|
| Are negative reviews longer? | |
| Average rating of verified / non-verified purchases | |
| Are most reviews positive or negative? | |

### Task 4 · Join tables

*The 10 most reviewed products, with their title and average rating, and the average rating per price band (under 20, 20 to 50, over 50 dollars).*

| Question | Answer |
|---|---|
| Reviews whose product is missing from the metadata (`left_anti`) | |

### Task 5 · Brands with unhappy customers (Spark SQL)

*Your SQL query, and its result: the 10 stores with the highest share of 1-2 star reviews (at least 5,000 reviews).*

### Task 6 · Reviews over time (charts)

![Reviews per year](output/task6_reviews_per_year.png)
![Average rating per year](output/task6_rating_per_year.png)

*3-5 sentences that explain the two charts.*

### Task 7 · Window functions

*The 3 products with the most reviews in each year from 2018 to 2023, with their title, and the growth of the number of reviews from the previous year, in %.*

*2-3 sentences of interpretation.*

## Phase B: execution and performance (Tasks 8-9)

### Task 8 · Performance experiment

*Run time of Task 3 on the full data. Run each setting 3 times and give the average. Time an action (`count()`, `collect()` or a write), not a transformation.*

| Setting | Partitions | Run 1 (s) | Run 2 (s) | Run 3 (s) | Average (s) |
|---|---|---|---|---|---|
| `local[1]`, `.jsonl.gz` | | | | | |
| `local[*]`, `.jsonl.gz` | | | | | |
| `local[*]`, Parquet copy | | | | | |

*One or two sentences per result: what changed, and why.*

*Why does the `.jsonl.gz` file give so few partitions?*

## Task 9 · Words of unhappy customers

*The 20 most frequent words of 50,000 negative reviews (1-2 stars) and of 50,000 positive reviews (5 stars), side by side.*

| Question | Answer |
|---|---|
| Words that appear only in the negative top 20 | |

*3-4 sentences: what do unhappy customers complain about?*

## Member contributions

| Member | Name | Tasks | Commits | Pull Requests | What I did |
|---|---|---|---|---|---|
| A | | 1-3 | | | |
| B | | 4-5 | | | |
| C | | 6-7 | | | |
| D | | 8 | | | |
| E  | | 9 | | | |
