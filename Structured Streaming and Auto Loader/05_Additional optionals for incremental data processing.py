# Databricks notebook source
# MAGIC %md
# MAGIC ## Creating a table

# COMMAND ----------

# MAGIC %sql
# MAGIC create table books
# MAGIC (id int,
# MAGIC name string,
# MAGIC author string)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books VALUES
# MAGIC   (1, 'Harry Potter and the Philosophers Stone', 'J.K. Rowling'),
# MAGIC   (2, 'The Great Gatsby', 'F. Scott Fitzgerald'),
# MAGIC   (3, 'To Kill a Mockingbird', 'Harper Lee'),  
# MAGIC   (4, 'Pride and Prejudice', 'Jane Austen'),
# MAGIC   (5, 'The Old Man and the Sea', 'Ernest Hemingway'),
# MAGIC   (6, 'The Catcher in the Rye', 'J.D. Salinger'),
# MAGIC   (7, 'Fahrenheit 451', 'Ray Bradbury'),
# MAGIC   (8, 'Of Mice and Men', 'John Steinbeck'),
# MAGIC   (9, 'ord of the Flies', 'William Golding'),
# MAGIC   (10, 'The Handmaids Tale', 'Margaret Atwood'),
# MAGIC   (11, 'Beloved', 'Toni Morrison'),
# MAGIC   (12, 'One Hundred Years of Solitude', 'Gabriel Garcia Marquez');

# COMMAND ----------

# MAGIC %sql
# MAGIC update books
# MAGIC set id = 20 where name = "nikita's diary"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from books

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended books

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading a stream from the above table

# COMMAND ----------

# creating a view from the table
spark.readStream.table("books").createOrReplaceTempView("books_streaming_temp_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from books_streaming_temp_view

# COMMAND ----------

## we have created a local table in the database, and made a streamable view of it

# COMMAND ----------

for s in spark.streams.active:
    print("Stopping " + s.id)
    s.stop()
    s.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Working with streaming data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT author, count(id) AS total_books
# MAGIC FROM books_streaming_temp_view
# MAGIC GROUP BY author

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unsupported operations

# COMMAND ----------

# MAGIC %sql
# MAGIC --- this type of sorting is not supported in streaming datasets. count is supported because of aggregation kind of function.
# MAGIC SELECT *
# MAGIC FROM books_streaming_temp_view
# MAGIC ORDER BY author

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persisting streaming results
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view books_temp as (
# MAGIC  SELECT author, count(id) AS total_books
# MAGIC FROM books_streaming_temp_view
# MAGIC GROUP BY author 
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from books_temp

# COMMAND ----------

# writing those results
(spark.table("books_temp")                               
      .writeStream  
      .trigger(processingTime='10 seconds')
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/FileStore/books/author_counts_checkpoint")
      .table("author_counts")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM author_counts

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC values
# MAGIC (13, 'Harry Potter and the Chamber of Secrets', 'J.K. Rowling'),
# MAGIC (14, 'The Casual Vacancy', 'J.K. Rowling'),
# MAGIC (15, 'Sense and Sensibility', 'Jane Austen');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from author_counts

# COMMAND ----------

for s in spark.streams.active:
    print("Stopping " + s.id)
    s.stop()
    s.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Last scenerio where we inserted the data after stopping the streaming and then writing the data into table

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC values
# MAGIC (16, 'Moby Dick', 'Herman Melville'),
# MAGIC (17, 'War and Peace', 'Leo Tolstoy'),
# MAGIC (18, 'Ready Player One', 'Ernest Cline');

# COMMAND ----------

# trigger(availableNow=True) will process all available data in multiple batches and then terminates the query
(spark.table("books_temp")                               
      .writeStream           
      .trigger(availableNow=True)
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/FileStore/books/author_counts_checkpoint")
      .table("author_counts")
      .awaitTermination()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM author_counts

# COMMAND ----------


