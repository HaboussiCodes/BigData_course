# Subject 4 · Book Recommender

[← Overview & rules](../README.md) · Files for your group's repository: [`starter/`](starter/) (`README.md` and `.gitignore`)

## Scenario and dataset

You are the data team of a reading app. Users rate the books they read. The head of product asks:

> "What do our readers like?"

You answer with Spark: query 6 million ratings with the PySpark DataFrame API. Calendar, Phase B, work distribution, deliverables and grading are in the [Overview & rules](../README.md) page.

| Repository | Notebook | Script (Task 8) |
| --- | --- | --- |
| `book-recommender-group-X` | `Book_Recommender_GroupX.ipynb` | `book_recommender_job.py` |

**Download:** the [goodbooks-10k dataset](https://github.com/zygmuntz/goodbooks-10k) on GitHub, built from Goodreads. No account is needed:

```bash
mkdir -p data && cd data
for f in ratings books to_read book_tags tags; do
  curl -O https://raw.githubusercontent.com/zygmuntz/goodbooks-10k/master/$f.csv
done
```

On Windows, run these commands in Git Bash, which comes with Git. The five files:

| File | Content | Size |
| --- | --- | --- |
| `ratings.csv` | `user_id`, `book_id`, `rating` (1 to 5) | 5,976,479 rows, 53,424 users, 10,000 books |
| `books.csv` | Titles, authors, publication year, Goodreads average rating | 10,000 rows |
| `to_read.csv` | `user_id`, `book_id` of books marked "to read" | 912,705 rows |
| `book_tags.csv` | `goodreads_book_id`, `tag_id`, `count` | 999,912 rows |
| `tags.csv` | `tag_id`, `tag_name` | 34,252 rows |

Columns used in this project:

| Column | Type | Use |
| --- | --- | --- |
| `user_id`, `book_id` | int | Joins |
| `rating` | int (1-5) | Analysis |
| `goodreads_book_id` | int | Joins `book_tags.csv` to `books.csv` |
| `authors`, `title`, `original_publication_year` | string, string, double | Analysis |
| `average_rating`, `ratings_count` | double, int | Goodreads' own average and number of ratings, for comparison |
| `tag_name`, `count` | string, int | Tags and genres (Tasks 4 and 7) |

## Phase A: PySpark queries (65 points)

Use the DataFrame API (`select`, `filter`, `groupBy`, `join`, `Window`) in every task, except Task 5, which uses Spark SQL.

### Task 1: Load and clean

Task 1 reads the five files and repairs `books.csv`. Put the CSV files in a folder `data/` next to your notebook, and start Spark with the first cell of the [Overview & rules](../README.md) page. Then follow the three steps, and complete the lines of the code marked `...`.

1. Read the five files with the code below, then print the schema of `books` and the number of rows of each file.
2. In the schema of `books`, `average_rating` and three count columns are strings instead of numbers. Show the rows whose `average_rating` is not a number (code below): quotes in their title shifted the columns. Then read `books.csv` again with `escape='"'` (hint 1), and check the schema again.
3. Add `first_author` to the books: the first name in `authors`, before the first comma.

```python
# Start with the first cell of the "Overview & rules" page: it creates spark and imports F.

# Step 1. header=True: the first line holds the column names.
#         inferSchema=True: numbers are read as numbers (without it, every column is a string).
ratings = spark.read.csv("data/ratings.csv", header=True, inferSchema=True)
books = spark.read.csv("data/books.csv", header=True, inferSchema=True)    # step 2 repairs this line
to_read = spark.read.csv("data/to_read.csv", header=True, inferSchema=True)
book_tags = spark.read.csv("data/book_tags.csv", header=True, inferSchema=True)
tags = spark.read.csv("data/tags.csv", header=True, inferSchema=True)

books.printSchema()
print("ratings:", ratings.count())
# ... the same for books, to_read, book_tags and tags

# Step 2. The broken rows: their average_rating is not a number
(books.filter(F.col("average_rating").cast("double").isNull())
      .select("book_id", "title", "average_rating").show(truncate=False))
# ... then read books.csv again with escape='"' added to the options, and print the schema again

# Step 3. F.split(F.col("authors"), ",") cuts the text at each comma; [0] keeps the first part
books = books.withColumn("first_author", ...)
books.select("title", "authors", "first_author").show(5)
```

**Deliverable:** the number of rows of each file, the broken rows, and `show(5)` of the books.

### Task 2: Select, filter and sort

- The 10 books with the highest Goodreads `average_rating`, among books with at least 100,000 `ratings_count`.
- How many books were first published before 1900? Show the 5 oldest (`original_publication_year` is negative before year 1).
- The books whose title contains "Harry Potter", sorted by publication year.

```python
books.filter(F.col("title").contains("Hobbit")).select("title", "authors").show()
```

**Requirement:** `select`, `filter`, `orderBy`, `limit` and `count` only.

### Task 3: Aggregate with groupBy

- The number of ratings for each star, from 1 to 5, and its share.
- The number of ratings of each user. Give the smallest, the median (`F.percentile_approx`) and the largest.
- For each book: number of ratings and average rating. Keep this table for Task 4.

**Requirement:** `groupBy` and `agg`, with an `alias` for every computed column.

### Task 4: Join tables

- Join the book table of Task 3 with `books.csv`. Show the 10 most rated books, with their average rating in the dataset and Goodreads' `average_rating`. Why can the two averages differ?
- Join `book_tags.csv` with `tags.csv`, and show the 10 tags used most often (the sum of `count`). Which of them are not genres?
- With a `left_anti` join, find the books that no user has marked "to read" in `to_read.csv`.

**Requirement:** `join` with `how="inner"` and with `how="left_anti"`.

### Task 5: Best authors (Spark SQL)

- With `spark.sql()`, list the 10 authors (`first_author`) with the highest average rating, among authors with at least 5 books and at least 10,000 ratings in the dataset.

**Requirement:** `spark.sql()` only, with a `JOIN`, a `GROUP BY` and a `HAVING`.

### Task 6: Books and ratings (charts)

- The number of books per publication decade, from 1900.
- The number of ratings per book, from the most to the least rated book (log scale).
- Convert each result with `.toPandas()` and draw two charts with matplotlib.

**Requirement:** explain the two charts in 3-5 sentences.

### Task 7: Window functions

- The genres are the tags `fantasy`, `romance`, `mystery`, `science-fiction` and `horror`. A book belongs to a genre when its tag `count` is at least 100. For each genre, show the 3 books with the highest average rating, among books with at least 1,000 ratings. Join the tags through `goodreads_book_id` (see hint 2).
- For each author, compute the share of each of their books in the author's ratings:

```python
w = Window.partitionBy("first_author")
per_book.withColumn("share", F.col("ratings") / F.sum("ratings").over(w))
```

Show the books of Suzanne Collins and of Harper Lee. Which authors owe most of their ratings to one book?

**Requirement:** `row_number()` over `Window.partitionBy(...).orderBy(...)` for the genres, and `F.sum(...).over(Window.partitionBy(...))` for the shares.

## Phase B and Task 10

- **Task 8:** the script is `book_recommender_job.py`. It runs Tasks 1 and 3 and writes the tables of Task 3 in `output/`. Run it with `spark-submit --master "local[*]" --driver-memory 4g book_recommender_job.py data/`.
- **Task 9:** time your Task 3 on `ratings.csv`, then on a Parquet copy of the same data.

### Task 10 (G7 only, 10 points): To-read lists

- Count how many users marked each book "to read", and show the 10 most wanted books with their titles.
- Join `to_read.csv` with `ratings.csv` on both `user_id` and `book_id` (see hint 4): how many to-read entries has the same user also rated? What can it mean?
- For the books with at least 100 ratings, compute the ratio (to-read count / number of ratings), and show the 10 books with the highest ratio. What kind of books are they?

**Deliverable:** the two tables, the count, and 3-4 sentences: how could the app use the to-read lists?

## Hints and common pitfalls

1. **Quotes inside fields.** A few titles in `books.csv` contain doubled quotes (`""`), which shift their columns. Read the file with `escape='"'`.
2. **Two different book IDs.** `ratings.csv` uses `book_id` (1 to 10,000), but `book_tags.csv` uses `goodreads_book_id`. Join the tags through `books.csv`.
3. **Tags are shelves.** Many tags, like `to-read` or `favorites`, are not genres.
4. **Join on two columns** with a list: `to_read.join(ratings, on=["user_id", "book_id"])`.
5. **`toPandas()` only on small results.** Aggregate first, then convert.
6. **A script is not a notebook.** `book_recommender_job.py` creates its own `SparkSession` and ends with `spark.stop()`.
