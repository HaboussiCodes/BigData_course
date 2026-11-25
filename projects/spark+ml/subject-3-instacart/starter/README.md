# Instacart Customer Segments · Group X

**Big Data with PySpark · group project · 22 November to 19 December 2026 · Subject 3**

> Replace **X** with your group number, in this file and in the file names.

You are the data team of an online grocery store. Before planning new promotions, the marketing director asks:

> *"How do our customers shop?"*

You answer with Spark: query 3.4 million orders with the PySpark DataFrame API.

This README has two parts. **Part 1** tells you how to start: the team, the data, the setup, the files, the tasks and the rules. **Part 2** is your project report: fill it in as you finish each task. The full instructions are in your teacher's repository: [Overview & rules](https://github.com/HaboussiCodes/BigData_course/tree/main/projects/spark%2Bml) and [Subject 3](https://github.com/HaboussiCodes/BigData_course/tree/main/projects/spark%2Bml/subject-3-instacart).

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

- [ ] Create the repository **`instacart-group-X`** on GitHub, and add all the members and your teacher (`HaboussiCodes`).
- [ ] Commit this `README.md` and the `.gitignore`.
- [ ] Download the data into `data/` (section 1).
- [ ] Every member installs the software and passes the quick check (section 2).
- [ ] Member A pushes Task 1 first: all the other tasks start from its clean data.

## 1. Get the data

"The Instacart Online Grocery Shopping Dataset 2017", published for the Kaggle competition [Instacart Market Basket Analysis](https://www.kaggle.com/c/instacart-market-basket-analysis/data). A free Kaggle account is needed, and you must accept the competition rules once before downloading. Create a free account on [kaggle.com](https://www.kaggle.com), open the link, and click **Download**. The files come as one `.zip`: unzip it into `data/`. If you find files ending in `.csv.zip` inside, unzip them too: Spark reads the `.csv` files.

| File | Content | Size |
|---|---|---|
| `orders.csv` | One row per order | 3,421,083 rows, 206,209 customers |
| `order_products__prior.csv` | Products in each prior order | 32,434,489 rows |
| `order_products__train.csv` | Products in each train order | 1,384,617 rows |
| `products.csv` | Product name, aisle and department IDs | 49,688 rows |
| `aisles.csv` | Aisle names | 134 rows |
| `departments.csv` | Department names | 21 rows |

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
instacart-group-X/
├── README.md                   <- this file, completed as your report
├── Instacart_GroupX.ipynb      <- Tasks 1-7 and 9 (and 10 for G7)
├── instacart_segments_job.py   <- Task 8
├── output/                     <- charts, logs, screenshots
├── data/                       <- the dataset, never committed
└── .gitignore
```

The notebook has one markdown header per task, with the code, the outputs, the charts and your written analysis. It must run from top to bottom without errors. Write the Parquet copies of Tasks 1 and 9 inside `data/`, never in `output/`.

## 4. How to run

**Notebook:** start `jupyter notebook`, open `Instacart_GroupX.ipynb`, then restart the kernel and run all the cells.

## 5. Tasks, owners and deadlines

| Task | What you do | Member | Push by |
|---|---|---|---|
| 1 | Load and inspect: read the four files used, count the orders of each `eval_set`, explain the nulls of `days_since_prior_order` | - | - |
| 2 | Select, filter, sort: organic products, morning orders, customers with 100 orders | - | - |
| 3 | Aggregate: basket sizes, orders per day of the week | - | - |
| 4 | Joins: build and save `lines`, reorder rate per department, products never ordered (`left_anti`) | - | - |
| 5 | Spark SQL: the 10 most popular products | - | - |
| 6 | Charts: orders per hour of the day and per day of the week |- | - |
| 7 | Window functions: the share of each product in its department, the top 3 products of each department | - | - |
| 8 | Performance: `local[1]` against `local[*]`, CSV against Parquet | - | - |
| 10 |  the products bought together | - | - |

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

> Fill in every section before the deadline. Replace the lines in *italics* and fill the empty cells. The numbers must be the same as in your notebook outputs. Save your charts in `output/`: the image links below already point there, so keep the same file names or change the links. The README report is worth 10 points: all sections present, clear conclusions.

## Executive summary

*2-3 sentences that answer the question of the scenario.*

## Phase A: main findings (Tasks 1-7)

### Task 1 · Load and inspect

| Question | Answer |
|---|---|
| Rows of `orders.csv` / `order_products__prior.csv` / `products.csv` / `departments.csv` | |
| Orders of each `eval_set` (prior, train, test) | |
| Nulls in `days_since_prior_order` / number of customers | |
| What do these nulls mean? | |

### Task 2 · Select, filter and sort

| Question | Answer |
|---|---|
| Products whose name contains "Organic" | |
| Orders placed between 8 and 11 a.m. | |
| Orders placed 30 days or more after the previous one | |
| Customers with the full 100 orders | |

### Task 3 · Aggregate with groupBy

| Question | Answer |
|---|---|
| Average / smallest / largest basket (products per order) | |

*The table per day of the week: number of orders and average `days_since_prior_order`.*

### Task 4 · Join tables

| Question | Answer |
|---|---|
| Rows of `lines`, saved as Parquet | |
| Products of `products.csv` that never appear in a prior order (`left_anti`) | |

*The table per department: number of lines and reorder rate.*

### Task 5 · Most popular products (Spark SQL)

*Your SQL query, and its result: the 10 products ordered most often (at least 1,000 times), with department, number of orders and reorder rate.*

### Task 6 · When do people shop? (charts)

![Orders per hour of the day](output/task6_orders_per_hour.png)
![Orders per day of the week](output/task6_orders_per_day.png)

*3-5 sentences that explain the two charts.*

### Task 7 · Window functions

*The 3 most ordered products of each department, with their share of the department.*

*2-3 sentences of interpretation.*

## Phase B: execution and performance (Tasks 8)

### Task 8 · Performance experiment

*Run time of Task 3 on the full data. Run each setting 3 times and give the average. Time an action (`count()`, `collect()` or a write), not a transformation.*

| Setting | Run 1 (s) | Run 2 (s) | Run 3 (s) | Average (s) |
|---|---|---|---|---|
| `local[1]`, CSV files | | | | |
| `local[*]`, CSV files | | | | |
| `local[*]`, Parquet copies | | | | |

![Spark UI: the stages of one job](output/spark_ui.png)

*One or two sentences per result: what changed, and why.*


## Task 9· Products bought together

*The 10 most frequent pairs of products in the same order (5% of the prior orders).*

| Question | Answer |
|---|---|
| Top pair (A, B) | |
| Share of the orders with A that also contain B | |

*3 ideas for the shop.*

## Member contributions

| Member | Name | Tasks | Commits | Pull Requests | What I did |
|---|---|---|---|---|---|
| A | | 1-3 | | | |
| B | | 4-5 | | | |
| C | | 6-7 | | | |
| D | | 8 | | | |
| E  | |9 | | | |
