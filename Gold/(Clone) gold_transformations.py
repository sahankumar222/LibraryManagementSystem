# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, when, countDistinct, desc, rank,datediff, weekofyear, month, year, dayofweek, lit,  concat
from pyspark.sql.window import Window

# COMMAND ----------

# Initialize Spark session
spark = SparkSession.builder.appName("GoldLayerTransformations").getOrCreate()

# COMMAND ----------


df_books = spark.read.table("`lms-catalog`.silver.books")
df_books_copies = spark.read.table("`lms-catalog`.silver.books_copies")
df_students = spark.read.table("`lms-catalog`.silver.students")
df_transactions = spark.read.table("`lms-catalog`.silver.transactions")
# display(df_books)
# display(df_books_copies)
# display(df_students)
# display(df_transactions)

# COMMAND ----------

# Join tables on book_id
df_joined = df_books.join(df_books_copies, "book_id", "inner").display()

# COMMAND ----------

# Total Books & Copies
total_books = df_books.select(countDistinct("book_id").alias("total_books")).collect()[0]["total_books"]
total_copies = df_books_copies.select(count("copy_id").alias("total_copies")).collect()[0]["total_copies"]
display(total_books, total_copies)

# COMMAND ----------

# Books by Department
df_books_by_department = df_books.groupBy("department").count().withColumnRenamed("count", "book_count")
display(df_books_by_department)

# COMMAND ----------

# Books by Availability
df_books_by_status = df_books_copies.groupBy("status").count().withColumnRenamed("count", "status_count")
display(df_books_by_status)

# COMMAND ----------

# Top Authors & Publishers
df_top_authors = df_books.groupBy("author").count().orderBy(desc("count")).limit(10)
df_top_publishers = df_books.groupBy("publisher").count().orderBy(desc("count")).limit(10)
df_top_authors.write.format("delta").mode("overwrite").saveAsTable("`lms-catalog`.gold.top_authors")
df_top_publishers.write.format("delta").mode("overwrite").saveAsTable("`lms-catalog`.gold.top_publishers")
# display(df_top_authors, df_top_publishers)

# COMMAND ----------

# Most Popular Books (Based on Copies)
window_spec = Window.orderBy(desc("copy_count"))
df_book_popularity = df_books_copies.groupBy("book_id").count().withColumnRenamed("count", "copy_count")
df_book_popularity = df_book_popularity.withColumn("rank", rank().over(window_spec)).limit(10)
df_book_popularity = df_book_popularity.join(df_books.select("book_id", "book_title"), "book_id")
df_book_popularity.show()
df_book_popularity.write.format("delta").mode("overwrite").saveAsTable("`lms-catalog`.gold.most_popular_books")



# COMMAND ----------

# Book Distribution by Location
df_location_distribution = df_books_copies.groupBy("location", "rack", "shelf").count().withColumnRenamed("count", "books_count")
df_location_distribution.show()

# COMMAND ----------

# Results Summary
print(f"Total Books: {total_books}, Total Copies: {total_copies}")

# COMMAND ----------


# Step 1: Join books_table and book_copies_table (Using INNER JOIN) -> books_final_table
books_final_table = df_books.join(df_books_copies, on="book_id", how="inner")


# COMMAND ----------

# Step 2: Join books_final_table and transaction_table (Using LEFT JOIN) -> books_transaction_table
books_transaction_table = books_final_table.join(df_transactions, on=["book_id", "copy_id"], how="left")
# books_transaction_table = books_final_table.join(df_transactions_q3, on=["book_id", "copy_id"], how="left")
# books_transaction_table = books_final_table.join(df_transactions_q4, on=["book_id", "copy_id"], how="left")
books_transaction_table.write.format("delta").mode("overwrite").saveAsTable("`lms-catalog`.gold.books_transactions_joins")

# display(books_transaction_table)


# COMMAND ----------

# Step 3: Join students_table and transaction_table (Using RIGHT JOIN) -> student_transaction_table
student_transaction_table = df_students.join(df_transactions, on="student_id", how="right")
# display(student_transaction_table)

# COMMAND ----------

student_transaction_table.columns

# COMMAND ----------

books_transaction_table.columns

# COMMAND ----------

# Step 4: Transformations on books_transaction_table
# Example: Count total transactions per book
book_transaction_counts = books_transaction_table.groupBy("book_title").agg(count("transaction_id").alias("total_transactions"))
book_transaction_counts.write.format("delta").mode("overwrite").saveAsTable("`lms-catalog`.gold.total_transactions_per_book")
# display(book_transaction_counts)


# COMMAND ----------

# Example: Calculate average fine per book
# avg_fine_per_book = books_transaction_table.groupBy("book_title").agg(avg("fine_amount").alias("average_fine"))
# avg_fine_per_book.write.format("delta").mode("overwrite").saveAsTable("`lms-catalog`.gold.avg_fine_per_book")
# display(avg_fine_per_book)
from pyspark.sql.functions import col, avg

# Calculate average fine per book for each final_status
avg_fine_per_book = books_transaction_table.groupBy("book_title", "final_status")\
    .agg(avg("fine_amount").alias("average_fine"))
avg_fine_per_book.write.format("delta").mode("overwrite").saveAsTable("`lms-catalog`.gold.avg_fine_per_book")
display(avg_fine_per_book)

# COMMAND ----------

# Example: Average fine per student
avg_fine_per_student = student_transaction_table.groupBy("student_id").agg(avg("fine_amount").alias("average_fine"))
avg_fine_per_book.write.format("delta").mode("overwrite").saveAsTable("`lms-catalog`.gold.avg_fine_per_student")
display(avg_fine_per_student)


# COMMAND ----------

# MAGIC %md
# MAGIC #Insights

# COMMAND ----------

# Read datasets from Azure Data Lake Storage
books_df = spark.read.format("delta").load("abfss://silver@lmsstorageaccount24.dfs.core.windows.net/books")
book_copies_df = spark.read.format("delta").load("abfss://silver@lmsstorageaccount24.dfs.core.windows.net/books_copies")
students_df = spark.read.format("delta").load("abfss://silver@lmsstorageaccount24.dfs.core.windows.net/students")
transactions_df = spark.read.table("`lms-catalog`.silver.transactions")


# COMMAND ----------

# Merging books_table, book_copies_table, and transactions_table for book insights using various joins
books_transaction_table = books_df.join(book_copies_df, "book_id", "inner")\
                                   .join(transactions_df, ["book_id", "copy_id"], "right")


# COMMAND ----------

books_transaction_table.write.format("delta").mode("overwrite").save("abfss://gold@lmsstorageaccount24.dfs.core.windows.net/books_transaction_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS `lms-catalog`.gold.books_transaction_table;
# MAGIC
# MAGIC CREATE TABLE `lms-catalog`.gold.books_transaction_table
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://gold@lmsstorageaccount24.dfs.core.windows.net/books_transaction_table';

# COMMAND ----------

# Merging students_table and transactions_table for student insights using different joins
students_transaction_table = students_df.join(transactions_df, "student_id", "outer")


# COMMAND ----------

students_transaction_table.display()

# COMMAND ----------

#Concatinating first_name and last_name as full_name

students_transaction_table = students_transaction_table.withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")))
students_transaction_table.display()

# COMMAND ----------

students_transaction_table.write.format("delta").mode("overwrite").save("abfss://gold@lmsstorageaccount24.dfs.core.windows.net/students_transaction_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS `lms-catalog`.gold.students_transaction_table;
# MAGIC
# MAGIC CREATE TABLE `lms-catalog`.gold.students_transaction_table
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://gold@lmsstorageaccount24.dfs.core.windows.net/students_transaction_table';

# COMMAND ----------

# Load the books_transaction_table
# books_transaction_table = spark.table("books_transaction_table")

# Generating insights for books
# from pyspark.sql.functions import count, col

books_transaction_table.groupBy("book_id").agg(
    count("transaction_id").alias("Total Borrows")
).orderBy(col("Total Borrows").desc()).show()
books_transaction_table.write.format("delta").mode("overwrite").saveAsTable("`lms-catalog`.gold.Total_Borrows")
display(avg_fine_per_book)


# COMMAND ----------

books_transaction_table.groupBy("department").agg(count("book_id").alias("Books Borrowed")).show()
books_transaction_table.write.format("delta").mode("overwrite").saveAsTable("`lms-catalog`.gold.Books_Borrowed_by_department")


# COMMAND ----------

books_transaction_table.withColumn("borrowing_duration", datediff(col("return_date"), col("issue_date")))\
    .groupBy("book_id").agg(avg("borrowing_duration").alias("avg_duration")).show()


# COMMAND ----------

# Create a new column to indicate if a book was damaged
transactions_df.withColumn("is_damaged", when(col("final_status") == "damaged", 1).otherwise(0))\
    .groupBy("is_damaged").count().show()
transactions_df.write.format("delta").mode("overwrite").saveAsTable("`lms-catalog`.gold.damaged_books_insights")



# COMMAND ----------

# transactions_df.filter(col("final_status") == "damaged")\
#     .groupBy("book_id").count().orderBy(col("count").desc()).show()
# transactions_df.write.format("delta").mode("overwrite").saveAsTable("`lms-catalog`.gold.most_damaged_books")
from pyspark.sql.functions import col, count, desc

# Filter only damaged books from transactions
damaged_books_df = transactions_df.filter(col("final_status") == "damaged")\
    .groupBy("book_id")\
    .agg(count("book_id").alias("damage_count"))\
    .orderBy(desc("damage_count"))

# Join with books table to get department info
damaged_books_with_dept = damaged_books_df.join(df_books.select("book_id", "book_title", "department"), "book_id", "left")

# Save to Delta table
# damaged_books_with_dept.write.format("delta").mode("overwrite").saveAsTable("lms-catalog.gold.most_damaged_books_with_dept")

# Display results
display(damaged_books_with_dept)




# COMMAND ----------


books_transaction_table.groupBy(month("issue_date").alias("month")).count().orderBy("month").show()
books_transaction_table.write.format("delta").mode("overwrite").saveAsTable("`lms-catalog`.gold.monthly_book_transactions")




# COMMAND ----------

books_transaction_table.groupBy(weekofyear("issue_date").alias("week")).count().orderBy("week").show()
books_transaction_table.write.format("delta").mode("overwrite").saveAsTable("`lms-catalog`.gold.weekly_book_transactions")


# COMMAND ----------

books_transaction_table.groupBy(dayofweek("issue_date").alias("day_of_week")).count().orderBy("day_of_week").show()
books_transaction_table.write.format("delta").mode("overwrite").saveAsTable("`lms-catalog`.gold.day_of_week")


# COMMAND ----------

display(students_transaction_table)

# COMMAND ----------

# # Ensure students_transaction_table is properly initialized
# students_transaction_table = spark.table("students_transaction_table")
# # Generating insights for students
# from pyspark.sql.functions import col
students_transaction_table.groupBy("student_id").count().orderBy(col("count").desc()).show()
students_transaction_table.write.format("delta").mode("overwrite").saveAsTable("`lms-catalog`.gold.students_borrowed_count")

# COMMAND ----------

students_transaction_table.select(sum("fine_amount").alias("Total Fine Collected")).show()


# COMMAND ----------

# students_transaction_table.withColumn("is_late", when(col("return_date") > col("due_date"), 1).otherwise(0))\
#     .groupBy("is_late").count().show()
# students_transaction_table.write.format("delta").mode("overwrite").saveAsTable("`lms-catalog`.gold.late_returns")
from pyspark.sql.functions import col, when, count, to_date, weekofyear

# Add a column to indicate late returns
students_transaction_table = students_transaction_table.withColumn("is_late", when(col("return_date") > col("due_date"), 1).otherwise(0))

# Daily Late Returns
daily_late_returns = students_transaction_table.filter(col("is_late") == 1)\
    .groupBy(to_date(col("return_date")).alias("return_date"))\
    .agg(count("is_late").alias("late_returns_count"))

# Weekly Late Returns
weekly_late_returns = students_transaction_table.filter(col("is_late") == 1)\
    .groupBy(weekofyear(col("return_date")).alias("week_number"))\
    .agg(count("is_late").alias("late_returns_count"))

# # Save to Delta Tables
# daily_late_returns.write.format("delta").mode("overwrite").saveAsTable("lms-catalog.gold.daily_late_returns")
# weekly_late_returns.write.format("delta").mode("overwrite").saveAsTable("lms-catalog.gold.weekly_late_returns")

# Display Results
display(daily_late_returns)
display(weekly_late_returns)


# COMMAND ----------

students_transaction_table.groupBy("student_id").agg(sum("fine_amount").alias("total_fine"))\
    .orderBy(col("total_fine").desc()).show()
students_transaction_table.write.format("delta").mode("overwrite").saveAsTable("`lms-catalog`.gold.students_fine_amount")



# COMMAND ----------

students_transaction_table.groupBy("student_id").agg(avg("fine_amount").alias("avg_fine"))\
    .orderBy(col("avg_fine").desc()).limit(10).show()


# COMMAND ----------

# students_transaction_table.filter(col("return_date") > col("due_date"))\
#     .groupBy("student_id").count().orderBy(col("count")).desc().show()
# students_transaction_table.write.format("delta").mode("overwrite").saveAsTable("`lms-catalog`.gold.students_late_returns")

students_transaction_table.filter(col("return_date") > col("due_date"))\
    .groupBy("student_id").count().orderBy(desc("count")).show()

students_transaction_table.write.format("delta").mode("overwrite").saveAsTable("`lms-catalog`.gold.students_late_returns")


# COMMAND ----------

# students_transaction_table.groupBy("student_id").count()\
#     .agg(avg("count").alias("avg_books_per_student")).display()
# students_transaction_table.write.format("delta").mode("overwrite").saveAsTable("`lms-catalog`.gold.avg_books_per_student")
from pyspark.sql.functions import avg, count, col, floor, ceil, concat_ws

# Calculate the average books borrowed per student
avg_books_df = students_transaction_table.groupBy("student_id").count()\
    .agg(avg("count").alias("avg_books_per_student"))

# Extract the floor (lower bound) and ceil (upper bound) of the average
avg_books_df = avg_books_df.withColumn("lower_bound", floor(col("avg_books_per_student")))\
                           .withColumn("upper_bound", ceil(col("avg_books_per_student")))

# Merge lower and upper bounds into a single column with a hyphen
avg_books_df = avg_books_df.withColumn("books_range", concat_ws("-", col("lower_bound"), col("upper_bound")))

# Show the result
avg_books_df.select("books_range").show(truncate=False)
avg_books_df.write.format("delta").mode("overwrite").saveAsTable("`lms-catalog`.gold.avg_books_per_student")

# COMMAND ----------

student_insights_df = students_transaction_table.groupBy("student_id").agg(
    count("transaction_id").alias("total_borrowed_books"),  # Total books borrowed per student
    sum("fine_amount").alias("total_fine_collected"),  # Total fine collected from each student
    avg("fine_amount").alias("average_fine_per_student"),  # Average fine per student
    count(when(col("return_date") > col("due_date"), 1)).alias("late_returns"),  # Late returns count
    countDistinct("book_id").alias("unique_books_borrowed"),  # Unique books borrowed
    count(when(col("final_status") == "damaged", 1)).alias("damaged_books_returned"),  # Books returned as damaged
    avg(datediff(col("return_date"), col("issue_date"))).alias("avg_borrowing_duration")  # Average borrowing duration
)

# Store the student insights as a Delta table in the catalog
student_insights_df.write.format("delta").mode("overwrite").saveAsTable("`lms-catalog`.gold.student_insights")


# COMMAND ----------

from pyspark.sql.functions import col, count, avg, datediff, when, month, weekofyear, dayofweek

books_insights_table = books_transaction_table.groupBy("book_id").agg(
    count("transaction_id").alias("Total_Borrows"),
    avg(datediff(col("return_date"), col("issue_date"))).alias("avg_duration"),
    count(when(col("final_status") == "damaged", 1)).alias("damaged_copies"),
    count(when(col("return_date") > col("due_date"), 1)).alias("late_returns")
).crossJoin(
    books_transaction_table.groupBy("department").agg(count("book_id").alias("Books_Borrowed")).withColumnRenamed("count", "department_count")
).crossJoin(
    books_transaction_table.groupBy(month("issue_date").alias("month")).count().alias("monthly_borrowing").withColumnRenamed("count", "monthly_count")
).crossJoin(
    books_transaction_table.groupBy(weekofyear("issue_date").alias("week")).count().alias("weekly_borrowing").withColumnRenamed("count", "weekly_count")
).crossJoin(
    books_transaction_table.groupBy(dayofweek("issue_date").alias("day_of_week")).count().alias("daily_borrowing").withColumnRenamed("count", "daily_count")
)

# Store the merged insights table in catalog
books_insights_table.write.mode("overwrite").saveAsTable("`lms-catalog`.gold.books_insights")

# COMMAND ----------

from pyspark.sql.functions import col, count, sum, avg, when, countDistinct, datediff

# Aggregate insights for each student
student_insights_df = students_transaction_table.groupBy("student_id").agg(
    count("transaction_id").alias("total_borrowed_books"),  # Total books borrowed per student
    sum("fine_amount").alias("total_fine_collected"),  # Total fine collected from each student
    avg("fine_amount").alias("average_fine_per_student"),  # Average fine per student
    count(when(col("return_date") > col("due_date"), 1)).alias("late_returns"),  # Late returns count
    countDistinct("book_id").alias("unique_books_borrowed"),  # Unique books borrowed
    count(when(col("final_status") == "damaged", 1)).alias("damaged_books_returned"),  # Books returned as damaged
    avg(datediff(col("return_date"), col("issue_date"))).alias("avg_borrowing_duration")  # Average borrowing duration
)

# Iterate over each student and save their insights as an individual table
for student in student_insights_df.collect():
    student_id = student["student_id"]
    student_df = student_insights_df.filter(col("student_id") == student_id)
    
    table_name = f"lms-catalog.gold.student_{student_id}"  # Unique table for each student
    student_df.write.format("delta").mode("overwrite").saveAsTable(table_name)

print("Student insights saved as individual tables.")

# COMMAND ----------

# MAGIC %md
# MAGIC #creating tables

# COMMAND ----------

# Perform an inner join on book_id
df_joined = df_books.join(df_books_copies, "book_id", "inner")

# Store the joined data as a Delta table in the given catalog path
df_joined.write.format("delta").mode("overwrite").saveAsTable("`lms-catalog`.gold.inner_join_on_book_id")

print("Joined book insights table saved successfully.")

# COMMAND ----------

from pyspark.sql.functions import countDistinct, count
from pyspark.sql import Row

# Calculate total books and total copies
total_books = df_books.select(countDistinct("book_id").alias("total_books")).collect()[0]["total_books"]
total_copies = df_books_copies.select(count("copy_id").alias("total_copies")).collect()[0]["total_copies"]

# Create a DataFrame with the insights
total_insights_df = spark.createDataFrame([Row(total_books=total_books, total_copies=total_copies)])

# Store the insights as a Delta table in the catalog
total_insights_df.write.format("delta").mode("overwrite").saveAsTable("`lms-catalog`.gold.total_books_copies")

print("Total books and copies insights table saved successfully.")

# COMMAND ----------

from pyspark.sql.functions import col

# Aggregate book count by department
df_books_by_department = df_books.groupBy("department").count().withColumnRenamed("count", "book_count")

# Store the insights as a Delta table in the catalog
df_books_by_department.write.format("delta").mode("overwrite").saveAsTable("`lms-catalog`.gold.books_by_department")
display(df_books_by_department)

# COMMAND ----------


