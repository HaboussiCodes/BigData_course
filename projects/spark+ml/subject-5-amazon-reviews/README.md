# Subject 5 · Amazon Review Sentiment

[← Overview & rules](../README.md) · Files for your group's repository: [`starter/`](starter/) (`README.md` and `.gitignore`)

## Scenario and dataset

You are the data team of an online video-game shop. Customers write millions of reviews, and the customer-care manager wants to find unhappy customers fast:

> "What do customers say about our games and consoles?"

You answer with Spark: query 4.6 million reviews with the PySpark DataFrame API. Calendar, Phase B, work distribution, deliverables and grading are in the [Overview & rules](../README.md) page.

| Repository | Notebook | Script (Task 8) |
| --- | --- | --- |
| `amazon-reviews-group-X` | `Amazon_Reviews_GroupX.ipynb` | `review_sentiment_job.py` |

**Download:** the Video_Games category of the Amazon Reviews 2023 dataset (McAuley Lab, UC San Diego), listed in the [dataset README](https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/blame/main/README.md). No account is needed. Do not unzip the files: Spark reads `.gz` directly.

```bash
mkdir -p data && cd data
curl -O https://datarepo.eng.ucsd.edu/mcauley_group/data/amazon_2023/raw/review_categories/Video_Games.jsonl.gz
curl -O https://datarepo.eng.ucsd.edu/mcauley_group/data/amazon_2023/raw/meta_categories/meta_Video_Games.jsonl.gz
```

On Windows, run these commands in Git Bash, which comes with Git. Each line of these files is one JSON record:

| File | Content | Size |
| --- | --- | --- |
| `Video_Games.jsonl.gz` | One review per line | 4.6 million reviews by 2.8 million users |
| `meta_Video_Games.jsonl.gz` | One product per line | 137,200 products |

Fields used in this project:

| Field | Type | Use |
| --- | --- | --- |
| `rating` | double (1.0 to 5.0) | Analysis |
| `title`, `text` | string | Analysis: the words of the review |
| `parent_asin` | string | Product ID, joins the metadata |
| `user_id` | string | Analysis |
| `timestamp` | long, Unix time in milliseconds (13 digits) | Dates |
| `helpful_vote`, `verified_purchase` | long, boolean | Analysis |
| Metadata: `title`, `store`, `price` | string, string, double | Analysis |

## Phase A: PySpark queries (65 points)

Use the DataFrame API (`select`, `filter`, `groupBy`, `join`, `Window`) in every task, except Task 5, which uses Spark SQL.

### Task 1: Load and clean

Task 1 reads the reviews, cleans them and saves a Parquet copy, which all the next tasks read (hint 1). Put the two `.jsonl.gz` files in a folder `data/` next to your notebook, and start Spark with the first cell of the [Overview & rules](../README.md) page. Then follow the five steps, and complete the lines of the code marked `...`.

1. Read the reviews with the code below, then print the schema and the number of rows. Spark reads the `.gz` file directly: do not unzip it.
2. Drop the `images` column.
3. Add a `date` and a `year` column from `timestamp`, which counts milliseconds (hint 2).
4. Remove the reviews whose text is empty.
5. Save the result as Parquet, and read it back into `reviews`. From then on, start your notebook from this Parquet copy.

```python
# Start with the first cell of the "Overview & rules" page: it creates spark and imports F.

# Step 1. Each line of the file is one JSON record: spark.read.json reads it, .gz included.
raw = spark.read.json("data/Video_Games.jsonl.gz")
raw.printSchema()
print("Rows before cleaning:", raw.count())

# Steps 2 to 4
clean = (raw.drop("images")                 # step 2
            .withColumn("date", ...)         # step 3, hint 2: F.to_date(F.from_unixtime(...))
            .withColumn("year", ...)         # step 3: F.year("date")
            .filter(...))                    # step 4: F.trim(F.col("text")) != ""
print("Rows after cleaning:", clean.count())

# Step 5. Save once. Next time, start the notebook from the read below (hint 1).
clean.write.mode("overwrite").parquet("data/reviews.parquet")
reviews = spark.read.parquet("data/reviews.parquet")
# reviews = reviews.sample(0.01, seed=42)   # uncomment while you develop: keeps 1% of the reviews
reviews.agg(F.min("date"), F.max("date")).show()    # the first and the last date
reviews.show(5)

# The product file, used from Task 4: read only the four fields needed (hint 5)
meta = (spark.read.schema("parent_asin STRING, title STRING, store STRING, price DOUBLE")
             .json("data/meta_Video_Games.jsonl.gz"))
```

**Deliverable:** the number of rows before and after cleaning, the first and the last date, and `show(5)`.

### Task 2: Select, filter and sort

- The 10 reviews with the most helpful votes (`helpful_vote`): rating, title, year and number of votes.
- The number and the share of reviews from verified purchases (`verified_purchase`).
- The number of reviews whose text contains the word "refund" (see hint 4), and how many of them have 1 or 2 stars.

```python
reviews.filter(F.lower(F.col("text")).contains("broken")).count()
```

**Requirement:** `select`, `filter`, `orderBy`, `limit` and `count` only.

### Task 3: Aggregate with groupBy

- The number and the share of reviews for each star rating.
- For each star rating, the average length of the text (`F.length("text")`). Are negative reviews longer?
- The average rating of verified and of non-verified purchases.

**Requirement:** `groupBy` and `agg`, with an `alias` for every computed column. Are most reviews positive or negative?

### Task 4: Join tables

- Join the reviews with the metadata on `parent_asin`. Show the 10 most reviewed products, with their title and average rating.
- With a `left_anti` join, count the reviews whose product is missing from the metadata.
- For the products with a price: the average rating per price band (under 20, 20 to 50, over 50 dollars), built with `F.when`.

**Requirement:** `join` with `how="inner"` and with `how="left_anti"`.

### Task 5: Brands with unhappy customers (Spark SQL)

- Register the reviews and the metadata as temporary views.
- With `spark.sql()`, list the 10 stores (the metadata column `store`) with the highest share of negative reviews (1 or 2 stars), among stores with at least 5,000 reviews.

**Requirement:** `spark.sql()` only, with a `JOIN` on `parent_asin`, a `GROUP BY` and a `HAVING`.

### Task 6: Reviews over time (charts)

- The number of reviews per year, and the average rating per year.
- Convert each result with `.toPandas()` and draw two charts with matplotlib.

**Requirement:** explain the two charts in 3-5 sentences.

### Task 7: Window functions

- For each year from 2018 to 2023, show the 3 products with the most reviews, with their title from the metadata.
- With the number of reviews per year, compute the growth from the previous year, in %:

```python
w = Window.orderBy("year")
per_year.withColumn("growth", (F.col("reviews") / F.lag("reviews").over(w) - 1) * 100)
```

**Requirement:** `row_number()` over `Window.partitionBy(...).orderBy(...)` for the products, and `lag()` for the growth.

## Phase B and Task 10

- **Task 8:** the script is `review_sentiment_job.py`. It runs Tasks 1 and 3 and writes the tables of Task 3 in `output/`. Run it with `spark-submit --master "local[*]" --driver-memory 4g review_sentiment_job.py data/`.
- **Task 9:** time your Task 3 on the original `.jsonl.gz` file, then on your Parquet copy. Print the number of partitions of each (`df.rdd.getNumPartitions()`).

### Task 10 (G7 only, 10 points): Words of unhappy customers

- Take 50,000 negative reviews (1-2 stars) and 50,000 positive reviews (5 stars).
- Split the text into words: `F.explode(F.split(F.lower("text"), "\\W+"))`. Remove the empty words, the words of 1 or 2 letters, and the stop words (`StopWordsRemover.loadDefaultStopWords("english")` gives the list).
- Count the 20 most frequent words of each group. Which words appear only in the negative top 20?

**Deliverable:** the two lists side by side, and 3-4 sentences: what do unhappy customers complain about?

## Hints and common pitfalls

1. **Convert to Parquet first.** A `.gz` file cannot be split, so Spark reads it with a single task. After Task 1, always read your Parquet copy.
2. **Timestamps are in milliseconds.** Divide by 1000 before `F.from_unixtime()`.
3. **Join on `parent_asin`.** The metadata is keyed by `parent_asin`, not by `asin`.
4. **Lower case before searching.** `F.lower(F.col("text")).contains("refund")` also finds "Refund" and "REFUND".
5. **Read the metadata with a schema**, as in Task 1: its `details` field has hundreds of keys, and `price` is not always a number.
6. **HTML in the text.** If `br` appears among your top words, remove tags such as `<br />` with `F.regexp_replace`.
7. **Text is heavy.** Work on a sample, and call `toPandas()` only on small results.
8. **A script is not a notebook.** `review_sentiment_job.py` creates its own `SparkSession` and ends with `spark.stop()`.
