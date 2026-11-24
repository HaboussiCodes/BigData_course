# Subject 7 · Mobile Money Fraud

[← Overview & rules](../README.md) · Files for your group's repository: [`starter/`](starter/) (`README.md` and `.gitignore`)

## Scenario and dataset

You are the data team of a mobile-money operator: customers pay and send money from their phone. Fraudsters take control of accounts, transfer the money to accounts they hold, then cash it out. The head of security asks:

> "What does fraud look like in our transactions?"

You answer with Spark: query 6.36 million transactions with the PySpark DataFrame API. Fraud is a very rare event: fewer than 2 transactions in 1,000 are fraudulent. Calendar, Phase B, work distribution, deliverables and grading are in the [Overview & rules](../README.md) page.

| Repository | Notebook | Script (Task 8) |
| --- | --- | --- |
| `mobile-money-fraud-group-X` | `Mobile_Money_Fraud_GroupX.ipynb` | `fraud_detection_job.py` |

**Download:** "Synthetic Financial Datasets For Fraud Detection" on Kaggle, at [kaggle.com/datasets/ealaxi/paysim1](https://www.kaggle.com/datasets/ealaxi/paysim1). A free Kaggle account is needed. The data comes from PaySim, a simulator built from one month of real logs of a mobile-money service in an African country. It is one CSV file whose name starts with `PS_`: about 6.36 million transactions over 31 days, of which 8,213 are frauds (0.13%).

Columns of the file:

| Column | Type | Use |
| --- | --- | --- |
| `step` | int: 1 step = 1 hour, 744 steps = 31 days | Hour and day |
| `type` | `CASH_IN`, `CASH_OUT`, `DEBIT`, `PAYMENT`, `TRANSFER` | Analysis |
| `amount` | double | Analysis |
| `nameOrig`, `nameDest` | string account IDs (names starting with `M` are merchants) | Analysis |
| `oldbalanceOrg`, `newbalanceOrig`, `oldbalanceDest`, `newbalanceDest` | double | Analysis (Task 10) |
| `isFraud` | 0/1 | Analysis |
| `isFlaggedFraud` | 0/1: the old rule, which flags transfers above 200,000 | Analysis |

## Phase A: PySpark queries (65 points)

Use the DataFrame API (`select`, `filter`, `groupBy`, `join`, `Window`) in every task, except Task 5, which uses Spark SQL.

### Task 1: Load and inspect

Task 1 reads the transactions into `tx`, the table that all the next tasks use. Put the CSV file in a folder `data/` next to your notebook, and start Spark with the first cell of the [Overview & rules](../README.md) page. Then follow the three steps, and complete the lines of the code marked `...`.

1. Read the file with the code below, then print its schema and its number of rows.
2. Count the nulls of each column. The code does it in one query, with one `count` per column.
3. Add `hour` and `day` with `withColumn` (hint 2). Step 1 is the first hour of day 1.

```python
# Start with the first cell of the "Overview & rules" page: it creates spark and imports F.

# Step 1. The * replaces the rest of the long file name.
#         header=True: the first line holds the column names.
#         inferSchema=True: numbers are read as numbers (without it, every column is a string).
tx = spark.read.csv("data/PS_*.csv", header=True, inferSchema=True)
# tx = tx.sample(0.05, seed=42)   # uncomment while you develop: keeps 5% of the rows (hint 1)
tx.printSchema()
print("Rows:", tx.count())

# Step 2. For each column c, count the rows where c is null
tx.select([F.count(F.when(F.col(c).isNull(), 1)).alias(c) for c in tx.columns]).show()

# Step 3. tx keeps its name and gets two more columns
tx = (tx.withColumn("hour", ...)     # hint 2
        .withColumn("day", ...)      # hint 2
        .cache())                    # keeps tx in memory, because every task reuses it
tx.select("step", "hour", "day", "type", "amount", "isFraud").show(5)
```

**Deliverable:** the number of rows, the number of nulls per column, and `show(5)` with `hour` and `day`.

### Task 2: Select, filter and sort

- The 10 largest transactions: `step`, `type`, `amount`, `nameOrig`, `nameDest`, `isFraud`.
- The number of frauds, and the number of transactions flagged by the old rule (`isFlaggedFraud = 1`).
- The types of the transactions that go to a merchant (`nameDest` starts with `M`, see hint 3): `select("type").distinct()`.

```python
tx.filter((F.col("type") == "TRANSFER") & (F.col("amount") > 1000000)).count()
```

**Requirement:** `select`, `filter`, `orderBy`, `limit`, `distinct` and `count` only.

### Task 3: Aggregate with groupBy

- For each type: number of transactions, average amount, number of frauds, fraud rate.
- Compare the old rule with the real frauds: `groupBy("isFraud", "isFlaggedFraud").count()`. How many frauds does the old rule catch, and how many does it miss?

**Requirement:** `groupBy` and `agg`, with an `alias` for every computed column. Which types carry fraud?

### Task 4: Join tables

PaySim has a single table, so this task joins it with a small table that you create, and with itself.

- Create a DataFrame with one row per type and a short description, for example `("CASH_OUT", "money taken out at an agent")`, with `spark.createDataFrame(rows, ["type", "description"])`. Join it with the type table of Task 3.
- Join the fraudulent `TRANSFER`s with the `CASH_OUT`s, on the destination of the transfer = the origin of the cash-out (see hint 4). How many destinations of a fraudulent transfer then make a cash-out? What does it tell you about the simulator?
- With a `left_anti` join, count the destination accounts that never send money (they never appear in `nameOrig`).

**Requirement:** `join` with `how="inner"` and with `how="left_anti"`.

### Task 5: Where does the money go? (Spark SQL)

- Register the DataFrame as a temporary view.
- With `spark.sql()`, list the 10 destination accounts that received the most fraudulent money, among accounts with at least one fraudulent incoming transaction. Show their number of incoming transactions, fraudulent and in total.

**Requirement:** `spark.sql()` only, with a `GROUP BY` and a `HAVING`. PaySim has a single table, so no `JOIN` is needed here.

### Task 6: When does fraud happen? (charts)

- The fraud rate per hour of the day, and the fraud rate per day.
- Convert each result with `.toPandas()` and draw two charts with matplotlib.

**Requirement:** explain the two charts in 3-5 sentences. At which hours and on which days is the fraud rate highest?

### Task 7: Window functions

- For each transaction type, show its 3 largest transactions, with `isFraud`. Are the largest transactions frauds? What is the largest amount of a fraud?
- Compute the fraudulent amount of each `step`, and its running total over the month:

```python
w = Window.orderBy("step").rowsBetween(Window.unboundedPreceding, 0)
per_step.withColumn("running_total", F.sum("fraud_amount").over(w))
```

On which day does the running total pass half of all the fraudulent money?

**Requirement:** `row_number()` over `Window.partitionBy(...).orderBy(...)` for the largest transactions, and `rowsBetween` for the running total.

## Phase B and Task 10

- **Task 8:** the script is `fraud_detection_job.py`. It runs Tasks 1 and 3 and writes the tables of Task 3 in `output/`. Run it with `spark-submit --master "local[*]" --driver-memory 4g fraud_detection_job.py data/`.
- **Task 9:** time your Task 3 on the CSV file, then on a Parquet copy of the same data.

### Task 10 (G7 only, 10 points): Fraud patterns by amount

- Build amount bands with `F.when`: under 1,000; 1,000 to 10,000; 10,000 to 100,000; 100,000 to 1 million; 1 to 10 million; over 10 million.
- For `TRANSFER` and `CASH_OUT`, compute the number of transactions, the number of frauds and the fraud rate of each band and type. Draw the fraud rate per band.
- Count the frauds whose amount is the whole balance of the origin account (`F.abs(F.col("amount") - F.col("oldbalanceOrg")) < 0.01`), and the honest transactions that do the same. What do fraudsters do with the accounts they take over?

**Deliverable:** the table, the chart, and 3-4 sentences of conclusions.

## Hints and common pitfalls

1. **Rare events need bigger samples.** Develop on 5% of the data, then run the final notebook on the full file.
2. **From step to hours.** One step is one hour: `hour` = `(F.col("step") - 1) % 24`, and `day` = `F.floor((F.col("step") - 1) / 24) + 1`.
3. **Merchants.** Accounts whose name starts with `M` are merchants: `F.col("nameDest").startswith("M")`. Their balances are always 0.
4. **Joining a table with itself.** Give the key the same name on both sides first: `select(F.col("nameDest").alias("account"))` for the transfers and `select(F.col("nameOrig").alias("account"))` for the cash-outs, then join `on="account"`.
5. **`toPandas()` only on small results.** Aggregate first, then convert.
6. **A script is not a notebook.** `fraud_detection_job.py` creates its own `SparkSession` and ends with `spark.stop()`.
