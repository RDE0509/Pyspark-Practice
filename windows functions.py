# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

from pyspark.sql.functions import *


# COMMAND ----------

spark = SparkSession.builder.appName('Windows function').getOrCreate()

# COMMAND ----------


data = (("James", "Sales", 3000), \
    ("Michael", "Sales", 4600),  \
    ("Robert", "Sales", 4100),   \
    ("Maria", "Finance", 3000),  \
    ("James", "Sales", 3000),    \
    ("Scott", "Finance", 3300),  \
    ("Jen", "Finance", 3900),    \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000),\
    ("Saif", "Sales", 4100) \
  )

# COMMAND ----------

columns= ["employee_name", "department", "salary"]

# COMMAND ----------

df = spark.createDataFrame(data = data, schema = columns)

# COMMAND ----------

df.show()

# COMMAND ----------

windowspec = Window.partitionBy('department').orderBy('salary')

# COMMAND ----------

df.withColumn("row_number",row_number().over(windowspec)).show()

# COMMAND ----------

df.withColumn('Rank',rank().over(windowspec)).show()

# COMMAND ----------

df.withColumn('dense_rank',dense_rank().over(windowspec)).show()

# COMMAND ----------

df.withColumn('percent_rank',percent_rank().over(windowspec)).show()

# COMMAND ----------

df.withColumn('ntile',ntile(2).percent_rank().over(windowspec)).show()

# COMMAND ----------

df.withColumn('lag',lag('salary',2).over(windowspec)).show()

# COMMAND ----------

df.withColumn('lead',lead('salary',2).over(windowspec)).show()

# COMMAND ----------

df.withColumn("row",row_number().over(windowspec)) \
  .withColumn("avg", avg(col("salary")).over(windowspec)) \
  .withColumn("sum", sum(col("salary")).over(windowspec)) \
  .withColumn("min", min(col("salary")).over(windowspec)) \
  .withColumn("max", max(col("salary")).over(windowspec)) \
  .where(col("row")==1).select("department","avg","sum","min","max") \
  .show()

# COMMAND ----------


