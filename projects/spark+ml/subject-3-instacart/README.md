# Subject 3 · Instacart Customer Segments

[← Overview & rules](../README.md) · Files for your group's repository: [`starter/`](starter/) (`README.md` and `.gitignore`)

## Scenario and dataset

You are the data team of an online grocery store. Before planning new promotions, the marketing director asks:

> "How do our customers shop?"

You answer with Spark: query 3.4 million orders with the PySpark DataFrame API. Calendar, Phase B, work distribution, deliverables and grading are in the [Overview & rules](../README.md) page.

| Repository | Notebook |
| --- | --- | --- |
| `instacart-group-X` | `Instacart_GroupX.ipynb` |
**Download:** "The Instacart Online Grocery Shopping Dataset 2017", published for the Kaggle competition [Instacart Market Basket Analysis](https://www.kaggle.com/c/instacart-market-basket-analysis/data). A free Kaggle account is needed, and you must accept the competition rules once before downloading. It has six CSV files:

| File | Content | Size |
| --- | --- | --- |
| `orders.csv` | One row per order | 3,421,083 rows, 206,209 customers |
| `order_products__prior.csv` | Products in each prior order | 32,434,489 rows |
| `order_products__train.csv` | Products in each train order | 1,384,617 rows |
| `products.csv` | Product name, aisle and department IDs | 49,688 rows |
| `aisles.csv` | Aisle names | 134 rows |
| `departments.csv` | Department names | 21 rows |

Columns used in this project:

| Column | Type | Use |
| --- | --- | --- |
| `order_id`, `user_id`, `product_id` | int | Keys for the joins |
| `eval_set` | string: prior, train or test | Filter |
| `order_number` | int (1 = the customer's first order) | Analysis |
| `order_dow` | int (0-6) | Analysis |
| `order_hour_of_day` | int (0-23) | Analysis |
| `days_since_prior_order` | double, capped at 30, null for a first order | Analysis |
| `reordered` | 0/1 (1 = bought before by this customer) | Analysis |
| `product_name`, `aisle`, `department` | string | Analysis |

## Phase A: PySpark queries (65 points)

Use the DataFrame API (`select`, `filter`, `groupBy`, `join`, `Window`) in every task, except Task 5, which uses Spark SQL.

### Task 1: Load and inspect

Task 1 reads the four files used in this project and checks them. Put the CSV files in a folder `data/` next to your notebook, and start Spark with the first cell of the [Overview & rules](../README.md) page. Then follow the three steps, and complete the lines of the code marked `...`.

1. Read `orders.csv`, `order_products__prior.csv`, `products.csv` and `departments.csv` with the code below, then print the number of rows of each file.
2. Count the orders of each `eval_set` (prior, train, test).
3. Count the nulls of `days_since_prior_order`, and compare with the number of customers. Explain the nulls (hint 3).

```python
# Start with the first cell of the "Overview & rules" page: it creates spark and imports F.

# Step 1. header=True: the first line holds the column names.
#         inferSchema=True: numbers are read as numbers (without it, every column is a string).
orders = spark.read.csv("data/orders.csv", header=True, inferSchema=True)
prior = spark.read.csv("data/order_products__prior.csv", header=True, inferSchema=True)
products = spark.read.csv("data/products.csv", header=True, inferSchema=True, escape='"')   # hint 4
departments = spark.read.csv("data/departments.csv", header=True, inferSchema=True)

# While you develop, uncomment the 3 lines below. They keep 5% of the customers,
# with all their orders and all the products of these orders (hint 1).
# users = orders.select("user_id").distinct().sample(0.05, seed=42)
# orders = orders.join(users, on="user_id")
# prior = prior.join(orders.select("order_id"), on="order_id")

print("orders:", orders.count())
# ... the same for prior, products and departments

# Step 2
orders.groupBy("eval_set").count().show()

# Step 3
print("Nulls:", orders.filter(F.col("days_since_prior_order").isNull()).count())
print("Customers:", orders.select("user_id").distinct().count())
```

**Deliverable:** the number of rows of each file, the `eval_set` counts, the number of nulls with your explanation, and `show(5)` of each file.

### Task 2: Select, filter and sort

- The products whose name contains "Organic": how many are there? Show 10 of them, sorted by name.
- The number of orders placed between 8 and 11 a.m. (`order_hour_of_day`), and the number of orders placed 30 days or more after the previous one.
- The dataset keeps between 4 and 100 orders per customer. How many customers have the full 100 orders?

```python
products.filter(F.col("product_name").contains("Organic")).count()
```

**Requirement:** `select`, `filter`, `orderBy`, `limit` and `count` only.

### Task 3: Aggregate with groupBy

- Basket size: count the products of each order in `order_products__prior` (`groupBy("order_id")`), then give the average, the smallest and the largest basket.
- For each day of the week (`order_dow`): number of orders and average `days_since_prior_order`.

**Requirement:** `groupBy` and `agg`, with an `alias` for every computed column.

### Task 4: Join tables

- Build `lines`: join `order_products__prior` with `orders` (on `order_id`), `products` (on `product_id`) and `departments` (on `department_id`). Keep `order_id`, `user_id`, `order_dow`, `order_hour_of_day`, `product_name`, `department` and `reordered`. Save it as Parquet: the next tasks read this file.
- For each department: number of lines and reorder rate (the average of `reordered`).
- With a `left_anti` join, count the products of `products.csv` that never appear in a prior order.

**Requirement:** `join` with `how="inner"` and with `how="left_anti"`.

### Task 5: Most popular products (Spark SQL)

- Register `order_products__prior`, `products` and `departments` as temporary views.
- With `spark.sql()`, list the 10 products ordered most often, among products ordered at least 1,000 times. Show the product name, its department, the number of orders and the reorder rate.

**Requirement:** `spark.sql()` only, with a `JOIN`, a `GROUP BY` and a `HAVING`.

### Task 6: When do people shop? (charts)

- The number of orders per hour of the day, and per day of the week (from `orders.csv`).
- Convert each result with `.toPandas()` and draw two charts with matplotlib.

**Requirement:** explain the two charts in 3-5 sentences.

### Task 7: Window functions

- Count the lines of each product in each department, and add the share of the product in its department:

```python
w = Window.partitionBy("department")
counts.withColumn("share", F.col("n") / F.sum("n").over(w))
```

- For each department, show its 3 most ordered products, with their share.

**Requirement:** `F.sum(...).over(Window.partitionBy(...))` for the share, and `row_number()` over `Window.partitionBy(...).orderBy(...)` for the top 3.

## Phase B and Task 10

- **Task 8:** the script is `instacart_segments_job.py`. It runs Tasks 1 and 3 and writes the two tables of Task 3 in `output/`. Run it with `spark-submit --master "local[*]" --driver-memory 4g instacart_segments_job.py data/`.
- **Task 9:** time your Task 3 on the CSV files, then on Parquet copies of the same files.

### Task 10 (G7 only, 10 points): Products bought together

- Take 5% of the prior orders (sample the distinct `order_id`s) and their lines in `lines`.
- Join these lines with themselves on `order_id` (see hint 6), and keep the pairs where the first product name is smaller than the second, so that each pair counts once.
- Count each pair, and show the 10 most frequent pairs.
- For the top pair (A, B), compute the share of the orders with A that also contain B.

**Deliverable:** the table of the 10 pairs, and 3 ideas for the shop.

## Hints and common pitfalls

1. **Develop on 5% of the customers.** Sample the `user_id`s, then join, so that each sampled customer keeps all their orders.
2. **Join once, then save as Parquet.** Joining 32 million lines takes minutes: do it once in Task 4.
3. **`days_since_prior_order`** is null for a customer's first order, and 30 means "30 days or more".
4. **Quotes in product names.** Task 1 reads `products.csv` with `escape='"'`. Without it, a name that contains quotes is read wrongly, and its columns can even shift.
5. **Join on the column name.** `a.join(b, on="order_id")` keeps one `order_id` column. A condition such as `a.order_id == b.order_id` keeps both, and later selects fail as ambiguous.
6. **Joining a table with itself.** Rename the columns of the second copy first (`product_name` becomes `product_name_2`), then join `on="order_id"`.
7. **`toPandas()` only on small results.** Aggregate first, then convert.
8. **A script is not a notebook.** `instacart_segments_job.py` creates its own `SparkSession` and ends with `spark.stop()`.
