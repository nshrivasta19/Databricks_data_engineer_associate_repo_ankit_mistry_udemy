# Databricks notebook source
# MAGIC %md
# MAGIC pyspark command link
# MAGIC https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/index.html

# COMMAND ----------

# importing datatypes from pyspark library 
from pyspark.sql.types import IntegerType, StringType, DoubleType, StructField, StructType

# define the path of the file
bankdata_streaming_path  = "/mnt/streaming-demo/streaming_dataset/bankdata_streaming.csv"

# define schema of the file
bankdata_schema = StructType([
                    StructField("CustomerId", IntegerType()),
                    StructField("Surname", StringType()),
                    StructField("CreditScore", IntegerType()),
                    StructField("Geography", StringType()),
                    StructField("Gender", StringType()),
                    StructField("Age", IntegerType()),
                    StructField("Tenure", IntegerType()),
                    StructField("Balance", DoubleType()),
                    StructField("NumOfProducts", IntegerType()),
                    StructField("HasCrCard", IntegerType()),
                    StructField("IsActiveMember", IntegerType()),
                    StructField("EstimatedSalary", DoubleType()),
                    StructField("Exited", IntegerType())
                    ]
                    )



# COMMAND ----------

# Now we have to create a streaming data frame from above mentioned path
# created a streaming dataframe to read the streaming data bankdata_sdf and readStream will read the data from the streaming file
bankdata_sdf = spark.readStream.csv(bankdata_streaming_path, bankdata_schema, header=True)

# COMMAND ----------

# command to verify if the dataframe is streaming or not
bankdata_sdf.isStreaming

# COMMAND ----------

# display the records using dataframe
# it is continue running to fetch the streaming data from the file and we can insert the data and it will automatically appears here
bankdata_sdf.display()

# COMMAND ----------

print(" hey.. see we can't run any command if there is any other command is running... But even though above command is running continuously this cell will run because the above cell is fetching streaming data")

# COMMAND ----------

# command to stop the streaming
for a in spark.streams.active:
    print("Stopping" + a.id)
    a.stop()
    a.awaitTermination()


# COMMAND ----------


