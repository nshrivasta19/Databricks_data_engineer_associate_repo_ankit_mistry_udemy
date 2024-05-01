-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 1) Importing the data

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls dbfs:/FileStore/

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import zipfile
-- MAGIC  
-- MAGIC # Define the path to the zip file
-- MAGIC zip_path = "/dbfs/FileStore/data_cleaning.zip"
-- MAGIC  
-- MAGIC # Extract the files from the zip file
-- MAGIC with zipfile.ZipFile(zip_path, "r") as zip_ref:
-- MAGIC     zip_ref.extractall("/dbfs/FileStore/data_cleaning_extracted/")

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls dbfs:/FileStore/data_cleaning_extracted/data_cleaning/

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 2) Create a table

-- COMMAND ----------

CREATE TABLE dirty_data AS
SELECT * FROM json.`${dataset.dirty_data}/FileStore/data_cleaning_extracted/data_cleaning/dirty_data.json`;

-- COMMAND ----------

SELECT * FROM dirty_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 3) Inspect Data

-- COMMAND ----------

SELECT count(user_id), count(user_first_touch_timestamp), count(email), count(updated), count(*)
FROM dirty_data

-- COMMAND ----------

SELECT
  count_if(user_id IS NULL) AS missing_user_ids, 
  count_if(user_first_touch_timestamp IS NULL) AS missing_timestamps, 
  count_if(email IS NULL) AS missing_emails,
  count_if(updated IS NULL) AS missing_updates
FROM dirty_data

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 138+848

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 4) Distinct Records

-- COMMAND ----------

SELECT DISTINCT(*)
FROM dirty_data

-- COMMAND ----------

SELECT count(DISTINCT(*))
FROM dirty_data

-- COMMAND ----------

SELECT count(DISTINCT(user_id))
FROM dirty_data

-- COMMAND ----------

SELECT count(DISTINCT(user_first_touch_timestamp))
FROM dirty_data

-- COMMAND ----------

SELECT 
  count(user_id) AS total_ids,
  count(DISTINCT user_id) AS unique_ids,
  count(email) AS total_emails,
  count(DISTINCT email) AS unique_emails,
  count(updated) AS total_updates,
  count(DISTINCT(updated)) AS unique_updates,
  count(*) AS total_rows, 
  count(DISTINCT(*)) AS unique_non_null_rows
FROM dirty_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 5) Deduplicate Rows

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW users_deduped AS
  SELECT DISTINCT(*) FROM dirty_data;
 
SELECT * FROM users_deduped

-- COMMAND ----------

SELECT COUNT(*) FROM users_deduped

-- COMMAND ----------

SELECT * FROM dirty_data
WHERE
  user_id IS NULL AND
  user_first_touch_timestamp IS NULL AND
  email IS NULL AND
  updated IS NULL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 6) Deduplicate Based on Specific Columns

-- COMMAND ----------

SELECT COUNT(DISTINCT(user_id, user_first_touch_timestamp))
FROM dirty_data
WHERE user_id IS NOT NULL

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW deduped_users AS
SELECT user_id, user_first_touch_timestamp, max(email) AS email, max(updated) AS updated
FROM dirty_data
WHERE user_id IS NOT NULL
GROUP BY user_id, user_first_touch_timestamp;
 
SELECT count(*) FROM deduped_users

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 7) Validate Datasets

-- COMMAND ----------

SELECT * FROM deduped_users

-- COMMAND ----------

 SELECT user_id, count(*) AS row_count
  FROM deduped_users
  GROUP BY user_id

-- COMMAND ----------

SELECT max(row_count) <= 1 no_duplicate_ids FROM (
  SELECT user_id, count(*) AS row_count
  FROM deduped_users
  GROUP BY user_id)

-- COMMAND ----------

SELECT max(user_id_count) <= 1 at_most_one_id FROM (
  SELECT email, count(user_id) AS user_id_count
  FROM deduped_users
  WHERE email IS NOT NULL
  GROUP BY email)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 8) Date Format and Regex

-- COMMAND ----------

SELECT *,
    CAST(user_first_touch_timestamp / 1e6 AS timestamp) AS first_touch 
  FROM deduped_users

-- COMMAND ----------

SELECT *,
  date_format(first_touch, "MMM d, yyyy") AS first_touch_date,
  date_format(first_touch, "HH:mm:ss") AS first_touch_time,
  regexp_extract(email, "(?<=@).+", 0) AS email_domain
FROM (
  SELECT *,
    CAST(user_first_touch_timestamp / 1e6 AS timestamp) AS first_touch 
  FROM deduped_users
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------


