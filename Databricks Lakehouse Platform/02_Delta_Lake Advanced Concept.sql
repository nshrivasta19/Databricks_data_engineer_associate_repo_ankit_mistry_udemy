-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Time Travel
-- MAGIC

-- COMMAND ----------

describe history employee

-- COMMAND ----------

select * from employee

-- COMMAND ----------

select * from employee version as of 1

-- COMMAND ----------

select * from employee@v1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Deleting the data from table

-- COMMAND ----------

delete from employee

-- COMMAND ----------

select * from employee

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## describe the history of the table and rollback to older version
-- MAGIC

-- COMMAND ----------

describe history employee

-- COMMAND ----------

select * from employee@v2

-- COMMAND ----------

restore table employee version as of 2

-- COMMAND ----------

select * from employee

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Command to view history of the table
-- MAGIC

-- COMMAND ----------

describe history employee

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Optimize command 
-- MAGIC This is the command to compact smaller sized files. In Big Data processing, having too many small files or too few very big files are not desired. It’s always good to have files with optimal size.
-- MAGIC
-- MAGIC As per OPTIMIZE, the default size is 1GB. So, for data files smaller than 1GB, running this command would combine such files to 1gb in size.

-- COMMAND ----------

describe detail employee

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/user/hive/warehouse/employee"

-- COMMAND ----------

optimize employee
zorder by (EmployeeID)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating a table from databricks location and optimize it
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS people_10m;
 
CREATE TABLE IF NOT EXISTS people_10m
AS SELECT * FROM delta.`/databricks-datasets/learning-spark-v2/people/people-10m.delta`;

-- COMMAND ----------

describe history people_10m

-- COMMAND ----------

INSERT INTO people_10m
VALUES 
  (1, "Hisako", "Isabella", "Malitrott", "F", 1961, 938-80-1874, 15482)

-- COMMAND ----------

DESCRIBE DETAIL people_10m

-- COMMAND ----------

describe history people_10m

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/people_10m'

-- COMMAND ----------

optimize people_10m
zorder by (gender)

-- COMMAND ----------

describe history people_10m

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Vaccum command -
-- MAGIC (*) Delta Tables, by default, keep the history files for time-travelling.
-- MAGIC
-- MAGIC (*) But over a period of time, if we do not clean up those data files, it will continue to pile-up huge amount of data, which is not good from maintenance and storage perspectives.
-- MAGIC
-- MAGIC (*) So, we must clean them periodically and for that we can use the VACUUM command, which helps us to remove the obsolete files, that are not part of our latest version of our delta table.
-- MAGIC
-- MAGIC We can remove the invalidated files which are:
-- MAGIC
-- MAGIC i. Not part of the present version of the table.
-- MAGIC
-- MAGIC ii. Files that have become invalid at least 7 days ago. (This is configurable)
-- MAGIC
-- MAGIC But before we remove these files, it’s always suggested to do a DRY RUN. This will list out all the files that will be removed after the actual VACUUM operation.
-- MAGIC
-- MAGIC (*) Since, we have recently added the files, the criteria of 7 day retention period won’t be fulfilled, so we will tweak it a bit:
-- MAGIC But this throws an error, because there’s a property set that is mandating a minimum retention period of 168 hours or 7 days for file obsolescence.
-- MAGIC
-- MAGIC (*) Thus, it lists down those files that became inactive in the last 1 hour.
-- MAGIC
-- MAGIC (*) We can also remove all the obsolete files by making RETAIN 0 HOURS. This means that even files that were just created or recently modified could be marked for deletion, if they are deemed unnecessary for query processing.
-- MAGIC
-- MAGIC

-- COMMAND ----------

vacuum people_10m

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/user/hive/warehouse/people_10m"

-- COMMAND ----------

vacuum people_10m retain 0 hours

-- COMMAND ----------

set spark.databricks.delta.retentionDurationCheck.enabled = false

-- COMMAND ----------

vacuum people_10m retain 0 hours

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/user/hive/warehouse/people_10m"

-- COMMAND ----------

describe history people_10m

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Deleting the table permanently

-- COMMAND ----------

drop table employee

-- COMMAND ----------

select * from employee

-- COMMAND ----------

drop table people_10m

-- COMMAND ----------

select* from people_10m

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employee'

-- COMMAND ----------


