# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
df = spark.read.options(delimiter=';').csv("dbfs:/mnt/MyFiles/championship_winners_V2.txt")
df = df.selectExpr("_c0 as year", "_c1 as name")
df_rn = df.select("year","name", F.row_number().over(Window.partitionBy("name").orderBy('year')).alias("row_num"))
df_diff = df_rn.withColumn("rn_diff", F.col("year") - F.col("row_num"))
df_final = df_diff.groupBy("name", "rn_diff").count().filter("count > 2")
#df_final.show()

# COMMAND ----------


#dbutils.fs.put("dbfs:///mnt/MyFiles/sample.txt", "sample content")
#df_diff.write.format("csv").save("dbfs:///mnt/MyFiles/sample.csv")
df_diff.write.mode("overwrite").format("json").save("/mnt/MyFiles/spark_training/")
#display(dbutils.fs.ls("/mnt/MyFiles/spark_training/"))

# COMMAND ----------

df = spark.read.json("/mnt/MyFiles/spark_training/part-00000-tid-567598966220459951-febc8306-8a5e-4fdf-9a41-fd38cf5efd08-21-1-c000.json")
df.printSchema()
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC df_final.write \
# MAGIC  .mode("overwrite") \
# MAGIC  .option("header", "true") \
# MAGIC  .csv("dbfs:/mnt/MyFiles/File_from_databricks.csv")
# MAGIC df_final.show()
# MAGIC 
# MAGIC df = spark.read.format("csv").option("recursiveFileLookup", "true").option("inferSchema", "true").option("header", "true").load("dbfs:/myfolder/sample/")
# MAGIC df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #Working_Append
# MAGIC df_final.select("name","count").write.mode("append").format("jdbc")\
# MAGIC     .option("url", "jdbc:sqlserver://mysqlserver12131.database.windows.net:1433;databaseName=mySampleDatabase") \
# MAGIC     .option("dbtable", "test_table_2") \
# MAGIC     .option("user", "azureuser").option("password", "Azure1!23").save()

# COMMAND ----------

# MAGIC %md
# MAGIC studentDf = spark.createDataFrame(
# MAGIC         [(1,'vijay',67),(2,'Ajay',88),(3,'jay',99),(4,'vinay',88)],
# MAGIC          ("id", "name", "marks"))
# MAGIC studentDf.select("id","name","marks").write.mode("append").format("jdbc")\
# MAGIC     .option("url", "jdbc:sqlserver://mysqlserver12131.database.windows.net:1433;databaseName=mySampleDatabase") \
# MAGIC     .option("dbtable", "students") \
# MAGIC     .option("user", "azureuser").option("password", "Azure1!23").save()
# MAGIC 
# MAGIC #df.write.mode("overwrite").save_as_table("table1")
