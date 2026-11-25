# Book Recommender · Group X

**Big Data with PySpark · group project · Subject 4**

> Replace **X** with your group number, in this file and in the file names.

You are the data team of a reading app. Users rate the books they read. The head of product asks:

> *"What do our readers like?"*

You answer with Spark: query 6 million ratings with the PySpark DataFrame API.

This README has two parts. **Part 1** tells you how to start: the team, the data, the setup, the files, the tasks and the rules. **Part 2** is your project report: fill it in as you finish each task. The full instructions are in your teacher's repository: [Overview & rules](https://github.com/HaboussiCodes/BigData_course/tree/main/projects/spark%2Bml) and [Subject 4](https://github.com/HaboussiCodes/BigData_course/tree/main/projects/spark%2Bml/subject-4-book-recommender).

# Part 1 · Getting started

## Team

| Member | Name | Student ID | GitHub username | Tasks |
|---|---|---|---|---|
| A | | | | 1-3: loading and cleaning, filtering and sorting, aggregation with `groupBy` |
| B | | | | 4-5: joins, Spark SQL |
| C | | | | 6-7: charts, window functions |
| D | | | | 8:  performance experiment |
| E | | | | 9 |

Your teacher gives each member one block of tasks. Each member commits the code of their own block, but everyone must understand the whole project: at the oral check, the teacher can ask any member about any task.

### Week 1 checklist

- [ ] Create the repository **`book-recommender-group-X`** on GitHub, and add all the members and your teacher (`HaboussiCodes`).
- [ ] Commit this `README.md` and the `.gitignore`.
- [ ] Download the data into `data/` (section 1).
- [ ] Every member installs the software and passes the quick check (section 2).
- [ ] Member A pushes Task 1 first: all the other tasks start from its clean data.

## 1. Get the data

The [goodbooks-10k dataset](https://github.com/zygmuntz/goodbooks-10k) on GitHub, built from Goodreads. No account is needed. Run these commands from the repository folder; on Windows, run them in Git Bash, which comes with Git:

```bash
mkdir -p data && cd data
for f in ratings books to_read book_tags tags; do
  curl -O https://raw.githubusercontent.com/zygmuntz/goodbooks-10k/master/$f.csv
done
cd ..
```

| File | Content | Size |
|---|---|---|
| `ratings.csv` | `user_id`, `book_id`, `rating` (1 to 5) | 5,976,479 rows, 53,424 users, 10,000 books |
| `books.csv` | Titles, authors, publication year, Goodreads average rating | 10,000 rows |
| `to_read.csv` | `user_id`, `book_id` of books marked "to read" | 912,705 rows |
| `book_tags.csv` | `goodreads_book_id`, `tag_id`, `count` | 999,912 rows |
| `tags.csv` | `tag_id`, `tag_name` | 34,252 rows |

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
book-recommender-group-X/
├── README.md                       <- this file, completed as your report
├── Book_Recommender_GroupX.ipynb   <- Tasks 1-7 and 9 (and 10 for G7)
├── book_recommender_job.py         <- Task 8
├── output/                         <- charts, logs, screenshots
├── data/                           <- the dataset, never committed
└── .gitignore
```

The notebook has one markdown header per task, with the code, the outputs, the charts and your written analysis. It must run from top to bottom without errors. Write the Parquet copies of Tasks 1 and 9 inside `data/`, never in `output/`.

## 4. How to run

**Notebook:** start `jupyter notebook`, open `Book_Recommender_GroupX.ipynb`, then restart the kernel and run all the cells.

## 5. Tasks, owners and deadlines

| Task | What you do | Member | Push by |
|---|---|---|---|
| 1 | Load and clean: read the five files, repair `books.csv` (`escape='"'`), add `first_author` | -|- |
| 2 | Select, filter, sort: the best-rated books, books before 1900, Harry Potter |-| - |
| 3 | Aggregate: ratings per star, ratings per user, ratings per book | - | - |
| 4 | Joins: the most rated books, the most used tags, books nobody wants to read (`left_anti`) | - | - |
| 5 | Spark SQL: the 10 best authors | - | - |
| 6 | Charts: books per decade, ratings per book |-| - |
| 7 | Window functions: the top 3 books of each genre, each book's share of its author's ratings | - | - |
| 8 | Performance: `local[1]` against `local[*]`, CSV against Parquet | - | - |
| 9 |  |  |  |

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

### Task 1 · Load and clean

| Question | Answer |
|---|---|
| Rows of `ratings.csv` / `books.csv` / `to_read.csv` / `book_tags.csv` / `tags.csv` | |
| Columns of `books.csv` read as strings without `escape='"'` | |
| The broken rows (`book_id` and title) | |

### Task 2 · Select, filter and sort

*The 10 books with the highest Goodreads `average_rating` (at least 100,000 `ratings_count`), and the "Harry Potter" books sorted by publication year.*

| Question | Answer |
|---|---|
| Books first published before 1900 | |
| The 5 oldest books | |

### Task 3 · Aggregate with groupBy

*The number and the share of ratings for each star, from 1 to 5.*

| Question | Answer |
|---|---|
| Ratings per user: smallest / median / largest | |

### Task 4 · Join tables

*The 10 most rated books, with their average rating in the dataset and Goodreads' `average_rating`, and the 10 tags used most often.*

| Question | Answer |
|---|---|
| Why can the two averages differ? | |
| Which of the top tags are not genres? | |
| Books that no user marked "to read" (`left_anti`) | |

### Task 5 · Best authors (Spark SQL)

*Your SQL query, and its result: the 10 authors with the highest average rating (at least 5 books and 10,000 ratings).*

### Task 6 · Books and ratings (charts)

![Books per publication decade](output/task6_books_per_decade.png)
![Ratings per book, log scale](output/task6_ratings_per_book.png)

*3-5 sentences that explain the two charts.*

### Task 7 · Window functions

*The 3 best-rated books of each genre (fantasy, romance, mystery, science-fiction, horror), and the share of each book in its author's ratings for Suzanne Collins and Harper Lee.*

*Which authors owe most of their ratings to one book?*

## Phase B: execution and performance (Tasks 8-9)

### Task 8 · Standalone script

| Item | Answer |
|---|---|
| Command used | `spark-submit --master "local[*]" --driver-memory 4g book_recommender_job.py data/` |
| Log | [`output/spark_submit/run.log`](output/spark_submit/run.log) |
| Tables written in `output/` | *the CSV folders of Task 3* |

### Task 9 · Performance experiment

*Run time of Task 3 on the full data. Run each setting 3 times and give the average. Time an action (`count()`, `collect()` or a write), not a transformation.*

| Setting | Run 1 (s) | Run 2 (s) | Run 3 (s) | Average (s) |
|---|---|---|---|---|
| `local[1]`, `ratings.csv` | | | | |
| `local[*]`, `ratings.csv` | | | | |
| `local[*]`, Parquet copy | | | | |

![Spark UI: the stages of one job](output/spark_ui.png)

*One or two sentences per result: what changed, and why.*

### Bonus (+5 points, optional) · Cluster execution

*If you ran the script on a real cluster (YARN or Spark standalone) or on a cloud notebook service, with the data on HDFS or cloud storage: give the command and the log.*

## Task 10 (G7 only) · To-read lists

*The 10 most wanted books (marked "to read" by the most users), and the 10 books with the highest ratio of to-read count / number of ratings (at least 100 ratings).*

| Question | Answer |
|---|---|
| To-read entries that the same user also rated | |
| What kind of books have the highest ratio? | |

*3-4 sentences: how could the app use the to-read lists?*

## Member contributions

| Member | Name | Tasks | Commits | Pull Requests | What I did |
|---|---|---|---|---|---|
| A | | 1-3 | | | |
| B | | 4-5 | | | |
| C | | 6-7 | | | |
| D | | 8-9 | | | |
| E (G7 only) | | 10 | | | |
