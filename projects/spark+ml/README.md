# PySpark Project: Flight Delay Analytics with Spark + MLlib

| Item | Details |
| --- | --- |
| Due date | [20december] |
| Group size | 2-4 students |
| Submission | One GitHub repository per group |
| Language | Python (PySpark 3.5+) |
| Total | 100 points |

## 1. Scenario: From Describing Delays to Predicting Them

You are the data team of an airline operations centre. The Operations Director has one year of flight records, about 5.8 million flights, and two questions:

> "Where and when do our delays happen? And can we predict that a flight will arrive late before it leaves the gate?"

Your mission is to answer both with Apache Spark:

1. Explore the data with Spark DataFrames and Spark SQL.
2. Build an MLlib pipeline that predicts whether a flight arrives 15 minutes late or more.
3. Compare three models and recommend one.
4. Package the pipeline as a script, run it with `spark-submit`, and measure how Spark settings change the run time.

## 2. Prerequisites and Local Setup

The whole project runs on a laptop in `local[*]` mode. No cluster is required.

**Knowledge:** the Spark lectures and labs on DataFrames, Spark SQL, and MLlib (`Pipeline`, `StringIndexer`, `VectorAssembler`, classifiers, evaluators).

**Software:**

| Software | Version | Install |
| --- | --- | --- |
| Python | 3.8+ | python.org/downloads |
| Java (JDK) | 11 or 17 | adoptium.net (Temurin) |
| PySpark | 3.5+ | `pip install pyspark` |
| Jupyter | any | `pip install jupyter` |
| pandas, matplotlib | any | `pip install pandas matplotlib` |

**Quick check:**

```bash
python3 --version     # 3.8+
java -version         # 11.x or 17.x, required by Spark
python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[*]').getOrCreate()
print(f'Spark {spark.version} is working!')
spark.stop()
"
```

**Common setup issues:**

| Problem | Solution |
| --- | --- |
| `java: command not found` | Install JDK 11 or 17 |
| `JAVA_HOME is not set` | Set `JAVA_HOME` to your JDK path |
| `Py4JJavaError` when Spark starts | Wrong Java version; PySpark 3.5 needs Java 11 or 17 |
| Logs flood the terminal | `spark.sparkContext.setLogLevel("WARN")` |
| `OutOfMemoryError` | Start with `.config("spark.driver.memory", "4g")` and work on a sample |

If you cannot install Java, Google Colab is an accepted fallback for the notebook (`!pip install pyspark`). Task 9 (`spark-submit`) must still run on a real machine in your group.

## 3. Dataset

The project uses the public "2015 Flight Delays and Cancellations" dataset from the US Department of Transportation, available on Kaggle. It has three CSV files:

| File | Content | Size |
| --- | --- | --- |
| `flights.csv` | One row per flight in 2015 | About 5.8 million rows |
| `airlines.csv` | Airline code and name | 14 rows |
| `airports.csv` | Airport code, name, city, state | About 320 rows |

Columns of `flights.csv` used in this project:

| Column | Type | Use |
| --- | --- | --- |
| `MONTH` | int | Feature + analysis |
| `DAY_OF_WEEK` | int (1 = Monday) | Feature + analysis |
| `AIRLINE` | string (code) | Feature + analysis, joins `airlines.csv` |
| `ORIGIN_AIRPORT` | string (code) | Feature + analysis, joins `airports.csv` |
| `DESTINATION_AIRPORT` | string (code) | Analysis (routes) |
| `SCHEDULED_DEPARTURE` | int, HHMM | Extract departure hour |
| `DISTANCE` | int (miles) | Feature |
| `ARRIVAL_DELAY` | int (minutes) | Builds the label |
| `CANCELLED`, `DIVERTED` | 0/1 | Filter these flights out |

**Label:** `label = 1` if `ARRIVAL_DELAY >= 15`, otherwise `0`.

**Development sample:** build and debug everything on a 1% sample, then run the final notebook on the full file.

```python
df = spark.read.csv("data/flights.csv", header=True, inferSchema=True)
dev = df.sample(0.01, seed=42)
```

Do not commit the CSV files to GitHub. Add `data/` to `.gitignore`.

## 4. Phase A: Spark DataFrame and SQL Analytics (30 points)

### Task 1: Load, Inspect, Clean

- Load `flights.csv` with a header. Print the schema and the row count.
- Count the nulls in `ARRIVAL_DELAY`. Explain where they come from.
- Remove cancelled and diverted flights.
- Add `DEP_HOUR` (0-23) from `SCHEDULED_DEPARTURE`, and the `label` column.

**Deliverable:** row count before and after cleaning, and `show(5)` of the cleaned DataFrame.

### Task 2: Delays by Airline (DataFrame API)

- For each airline: number of flights, average arrival delay, percentage of delayed flights.
- Join with `airlines.csv` to show the airline name instead of the code.

```python
df.groupBy("AIRLINE").agg(
    F.count("*").alias("flights"),
    F.avg("ARRIVAL_DELAY").alias("avg_delay"),
    F.avg("label").alias("delay_rate"),
).orderBy(F.col("delay_rate").desc()).show()
```

**Requirement:** DataFrame API only. Which airline is the most punctual? The least?

### Task 3: Airport Hotspots (Spark SQL)

- Register the DataFrames as temporary views.
- With `spark.sql()`, list the 10 origin airports with the highest delay rate, among airports with at least 10,000 departures. Show the airport name and city from `airports.csv`.

**Requirement:** `spark.sql()` only, with a `JOIN`, a `GROUP BY` and a `HAVING`.

### Task 4: When Do Delays Happen? (DataFrame + Visualization)

- Delay rate per month, per day of week, and per departure hour.
- Convert each result with `.toPandas()` and plot it with matplotlib: three charts.

**Requirement:** interpret the charts in 3-5 sentences. Why does the delay rate change during the day?

### Task 5: Worst Routes per Airline (Window Function)

- A route is a pair (`ORIGIN_AIRPORT`, `DESTINATION_AIRPORT`).
- For each airline, show its 3 routes with the highest average arrival delay, among routes with at least 500 flights.

**Requirement:** use `Window.partitionBy(...).orderBy(...)` with `row_number()` or `rank()`.

## 5. Phase B: Spark MLlib, Delay Prediction (35 points)

Build a complete ML pipeline that predicts `label` (arrival delay of 15 minutes or more).

**Rule: no data leakage.** The prediction is made before departure. Only columns known before departure are allowed as features. `DEPARTURE_DELAY`, `TAXI_OUT`, `AIR_TIME` and all other delay columns are forbidden.

### Task 6: Feature Engineering Pipeline

Build a Spark ML `Pipeline` with:

- A new column `origin_grp`: the airport code for the 50 busiest origin airports, `"OTHER"` for the rest.
- `StringIndexer` for `AIRLINE` and `origin_grp`, with `handleInvalid="skip"`.
- `VectorAssembler` combining `MONTH`, `DAY_OF_WEEK`, `DEP_HOUR`, `DISTANCE`, `airline_index`, `origin_index` into `features`.
- Train/test split: 80/20 with `seed=42`.

Train on a 10% sample of the cleaned data (`df.sample(0.10, seed=42)`) so that training fits in laptop memory.

**Deliverable:** the `features` column for 5 rows, with an explanation of each position in the vector. Report the share of delayed flights in the training set.

### Task 7: Train and Evaluate Three Models

| Model | Key parameters |
| --- | --- |
| Logistic Regression | `maxIter=100`, `regParam=0.01` |
| Random Forest | `numTrees=50`, `maxDepth=8`, `maxBins=64` |
| Gradient-Boosted Trees | `maxIter=30`, `maxDepth=5`, `maxBins=64` |

For each model, report on the test set:

- AUC-ROC, accuracy, F1, precision, recall
- Confusion matrix (TN, FP, FN, TP)
- Training time in seconds

Also report the accuracy of a baseline that always predicts "on time".

**Deliverable:** one comparison table with the baseline and the three models. Answer: why is accuracy alone a misleading metric on this dataset?

### Task 8: Interpretation and Tuning

- Extract and plot the feature importances of the Random Forest.
- Tune the Random Forest with `CrossValidator` (3 folds) on a grid of at least 4 combinations of `maxDepth` and `numTrees`. Report the best parameters and the AUC gain.
- Save the best model with `model.write().overwrite().save("output/best_model")`.

**Deliverable:** bar chart of importances, plus written answers:

1. Which feature is the most important? Does it agree with your findings in Tasks 2-4?
2. Why does Logistic Regression score lower than the tree models here?
3. Which model do you recommend to the Operations Director, and why?

## 6. Phase C: Execution and Performance (20 points)

### Task 9: Standalone Script with spark-submit

- Convert Phase B (Tasks 6-8, without the cross-validation) into a standalone script `flight_delay_ml.py`.
- The script creates its own `SparkSession`, reads the data path from the command line, runs the pipeline, and prints the comparison table.

```bash
spark-submit \
    --master "local[*]" \
    --driver-memory 4g \
    flight_delay_ml.py data/flights.csv
```

**Evidence:** full terminal output saved to `output/spark_submit/run.log`.

### Task 10: Performance Experiment

Measure the run time of Task 2 on the full dataset under these settings. Run each one 3 times and report the average.

| Experiment | Settings to compare |
| --- | --- |
| Parallelism | `local[1]`, `local[2]`, `local[*]` |
| File format | CSV, then the same data saved and re-read as Parquet |
| Caching | Run Tasks 2-4 in sequence, with and without `df.cache()` |
| Shuffle partitions | `spark.sql.shuffle.partitions` = 8, 200 |

**Deliverable:** one results table, a screenshot of the Spark UI (`localhost:4040`) showing the stages of one job, and a short explanation of each result.

### Bonus: Cluster Execution (+5 points)

Run `flight_delay_ml.py` on a real cluster (YARN or Spark standalone) or on a cloud notebook service, with the data on HDFS or cloud storage. Provide the command and the log.

## 7. Work Distribution and Git Workflow

### One Student, One Set of Tasks

Every member must commit code for their assigned tasks. Suggested distribution for a 4-person team:

| Member | Tasks | Phase |
| --- | --- | --- |
| Member A | Tasks 1-3 (cleaning, airlines, Spark SQL) | A |
| Member B | Tasks 4-5 (temporal analysis, window function) | A |
| Member C | Tasks 6-8 (ML pipeline, evaluation, tuning) | B |
| Member D | Tasks 9-10 (script, performance experiment) | C |

### Put Your Name on Your Code

Every notebook cell or script section starts with a header naming its author:

```python
# ============================================
# Task 6: Feature Engineering Pipeline
# Author: First Last (ID: 12345)
# ============================================
```

If your name appears on no code, you are treated as a non-contributor.

### Git Workflow

1. Create a branch for your task: `git checkout -b task6-yourname`
2. Commit your work in small steps as you progress.
3. Push the branch and open a Pull Request to `main`.
4. Another member reviews and merges it.

Your GitHub username must appear in the commit history with meaningful code changes. One large commit at the end does not count.

## 8. Deliverables

1. **Notebook** `Flight_Delay_GroupX.ipynb`: Tasks 1-8 and 10 with code, output, charts and written analysis, one markdown header per task.
2. **Script** `flight_delay_ml.py`: standalone Phase B pipeline, runnable with `spark-submit`.
3. **README.md**, the project report:
   - Team members (names + IDs)
   - Executive summary (2-3 sentences)
   - Main findings of Phase A, with the charts
   - ML results: comparison table, best model, interpretation
   - Performance experiment table and conclusions
   - Member contributions table

Add the instructor ([GitHub username]) as a collaborator on the repository.

```
flight-delay-group-X/
├── README.md
├── Flight_Delay_GroupX.ipynb
├── flight_delay_ml.py
├── output/          <- charts, logs, screenshots, saved model
├── data/            <- CSV files, not committed (.gitignore)
└── .gitignore
```

## 9. Evaluation Criteria

### Group Score

| Component | Points | Details |
| --- | --- | --- |
| Phase A: DataFrame and SQL analytics (Tasks 1-5) | 30 | Correct results, Spark SQL and window function used, interpretation |
| Phase B: ML pipeline (Tasks 6-8) | 35 | No leakage, 3 models evaluated, tuning, interpretation |
| Phase C: Execution and performance (Tasks 9-10) | 20 | Working `spark-submit`, measured and explained results |
| README report | 10 | All sections present, clear conclusions |
| Git workflow | 5 | Branches and Pull Requests used |
| **Total** | **100** | Bonus: +5 for cluster execution |

### Individual Adjustment

The group score is the base. Each member's grade is adjusted by their personal GitHub contribution and by the oral check.

| Contribution level | Share of group score |
| --- | --- |
| Strong: regular commits, PRs, code authorship | 100% |
| Moderate: some commits, took part in reviews | 70-90% |
| Minimal: few or trivial commits | 30-60% |
| No commits | 0% |

During the oral check, each student explains a part of their own code chosen by the instructor. A student who cannot explain their code receives zero.

## 10. Hints and Common Pitfalls

1. **Start on the 1% sample.** Debugging on 58,000 rows is far faster than on 5.8 million. Switch to the full file only when the code works.
2. **Extracting the hour.** `SCHEDULED_DEPARTURE` is an integer in HHMM form (5 means 00:05, 1430 means 14:30). Use `F.floor(F.col("SCHEDULED_DEPARTURE") / 100)`.
3. **October airport codes.** In the October rows, airports appear as 5-digit numeric IDs instead of 3-letter codes. Detect this in Task 1 and state how you handle it (for example, exclude those rows from airport analyses).
4. **Label must be numeric.** MLlib expects 0/1, not a boolean: `F.when(F.col("ARRIVAL_DELAY") >= 15, 1).otherwise(0)`.
5. **`maxBins` for tree models.** A tree needs `maxBins` at least equal to the number of categories of every indexed feature. This is why Task 6 groups airports into 51 values and Task 7 sets `maxBins=64`.
6. **Imbalanced classes.** Most flights are on time. Look at precision, recall and the confusion matrix, not only accuracy.
7. **Cache the training data.** `train_df.cache()` avoids re-reading the CSV at every iteration.
8. **Spark is lazy.** A transformation takes no time; only an action (`count()`, `show()`, `collect()`) triggers the work. Time the action in Task 10.
9. **`toPandas()` only on small results.** Aggregate first, then convert. Never call it on the full DataFrame.
10. **A script is not a notebook.** `flight_delay_ml.py` cannot use `display()` or notebook variables, and must end with `spark.stop()`.

## 11. AI Usage Policy

- **Allowed:** debugging errors, conceptual explanations, generating comments.
- **Prohibited:** generating entire tasks or the whole pipeline.
- **Penalty:** a student who cannot explain their code during the oral check receives zero.
