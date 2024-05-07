# Databricks notebook source
# we are appending one by one records in our streaming data container to perform live streaming of the data by this simulator.. based on some filteration
# we are reading data from full_dataset and it will insert record one by one live mode in streaming_dataset

# COMMAND ----------

# creating a variable for path of the file inside full_dataset
bankdata_full_path = '/mnt/streaming-demo/full_dataset/bank_data.csv'

bankdata_full = spark.read.csv(bankdata_full_path, header=True)

# COMMAND ----------

# Fetching the data from full_dataset
bankdata_full.display()

# COMMAND ----------

spark.read.csv(bankdata_full_path, header=True).display()

# COMMAND ----------

# Applying Transformations on the data and getting each record in live mode in streaming_dataset|

# COMMAND ----------

bankdata_full.filter(bankdata_full['CustomerId']==1).display()

# COMMAND ----------

bankdata_streaming_path = '/mnt/streaming-demo/streaming_dataset/bankdata_streaming.csv'

# COMMAND ----------

# Filtering data and writing into streaming format
bankdata_1 = bankdata_full.filter(bankdata_full['CustomerId']==1)
bankdata_1.write.options(header=True).mode('append').csv(bankdata_streaming_path)

# COMMAND ----------

bankdata_streaming = spark.read.csv(bankdata_streaming_path, header=True)
bankdata_streaming.display()

# COMMAND ----------

bankdata_2 = bankdata_full.filter(bankdata_full['CustomerId']==2)
bankdata_2.write.options(header=True).mode('append').csv(bankdata_streaming_path)



# COMMAND ----------

bankdata_3 = bankdata_full.filter(bankdata_full['CustomerId']==3)
bankdata_3.write.options(header=True).mode('append').csv(bankdata_streaming_path)

# COMMAND ----------

bankdata_4_5 = bankdata_full.filter( (bankdata_full['CustomerId']==4) | (bankdata_full['CustomerId']==5))
bankdata_4_5.write.options(header=True).mode('append').csv(bankdata_streaming_path)

# COMMAND ----------

bankdata_6_7 = bankdata_full.filter( (bankdata_full['CustomerId']==6) | (bankdata_full['CustomerId']==7))
bankdata_6_7.write.options(header=True).mode('append').csv(bankdata_streaming_path)

# COMMAND ----------

bankdata_10_11 = bankdata_full.filter( (bankdata_full['CustomerId']==10) | (bankdata_full['CustomerId']==11))
bankdata_10_11.write.options(header=True).mode('append').csv(bankdata_streaming_path)

# COMMAND ----------

bankdata_12 = bankdata_full.filter(bankdata_full['CustomerId']==12)

# COMMAND ----------

bankdata_12 = bankdata_full.filter(bankdata_full['CustomerId']==12)

# COMMAND ----------

bankdata_streaming = spark.read.csv(bankdata_streaming_path, header=True)
bankdata_streaming.display()

# COMMAND ----------

# removing all the data from bankdata_streaming_path for now
dbutils.fs.rm(bankdata_streaming_path, recurse=True)

# COMMAND ----------

bankdata_8 = bankdata_full.filter(bankdata_full['CustomerId']==8)

# COMMAND ----------

bankdata_12 = bankdata_full.filter(bankdata_full['CustomerId']==12)
bankdata_12.write.options(header=True).mode('append').csv(bankdata_streaming_path)

# COMMAND ----------

bankdata_13 = bankdata_full.filter(bankdata_full['CustomerId']==13)
bankdata_13.write.options(header=True).mode('append').csv(bankdata_streaming_path)

# COMMAND ----------


