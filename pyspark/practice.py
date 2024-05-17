# Databricks notebook source
from pyspark.sql.functions import explode, split, col, avg, when

# COMMAND ----------

data = [("John", "python, sql"), ("Aravind", "Java,SQL,HTML"), ("Sridevi", "Python,sql,pyspark")]
df = spark.createDataFrame(data, ["Name", "Skills"])

# COMMAND ----------

df = df.withColumn("Skills",explode(split(col("Skills"),",")))
display(df)

# COMMAND ----------

display(df)

# COMMAND ----------

data1 = [
    ("Aravind", None, None),
    ("John", None, None),
    (None, "Sridevi", None)
]
df1 = spark.createDataFrame(data1, ["Name1", "Name2", "Name3"])

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import coalesce, col
data = [
    ("Aravind", None, None),
    ("John", None, None),
    (None, "Sridevi", None)
]
schema = StructType([StructField("Name1", StringType(), True),
                     StructField("Name2", StringType(), True),
                     StructField("Name3", StringType(), True)])
df = spark.createDataFrame(data, schema)
df = df.select(coalesce(col("Name1"), col("Name2"), col("Name3")).alias("Names"))
display(df)

# COMMAND ----------

data1 = [ (101, "Aravind"), (102, "John"), (103, "Sridevi") ]
data2 = [ ("pyspark", 90, 101), ("sql", 70, 101), ("pyspark", 70, 102), ("sql", 60, 102), ("sql", 30, 103), ("pyspark", 20, 103) ]

df1 = spark.createDataFrame(data1, ["student_id", "student_name"])
df2 = spark.createDataFrame(data2, ["course_name", "marks", "student_id"])
percentage_df = df2.groupBy("student_id").agg(avg("marks").alias('percentage'))
joined_df = df1.join(percentage_df, df1.student_id == percentage_df.student_id, 'inner').drop(percentage_df.student_id)
result_df = joined_df.withColumn("result", when(col("percentage") >= 70, "distinction")\
    .when(col("percentage") >= 60, 'first class')\
        .when(col("percentage") >= 50, 'second class')\
            .when(col("percentage") >= 40, 'third class')\
                .when(col("percentage") <= 39, 'fail'))
display(result_df)


# COMMAND ----------

from pyspark.sql.functions import concat_ws, col

# COMMAND ----------

data = [
    (1, 'John', None, 'Doe'),
    (2, 'Alice', 'Ann', 'Smith'),
    (3, 'Mike', None, 'Johnson')
]
schema = ["id","first_name","middle_name","last_name"]
df = spark.createDataFrame(data, schema)
full_name_df = df.withColumn("full_name", concat_ws(" ",col("first_name"), col("middle_name"), col("last_name")))
display(full_name_df.select("full_name"))

# COMMAND ----------

data = [
    ("John", "Apple", 10),
    ("Alice", "Apple", 20),
    ("John", "Banana", 12),
    ("Alice", "Banana", 14),
    ("Mike", "Apple", 15),
    ("Mike", "Banana", 17)
]
schema = ["salesperson", 'product', 'quality']
df = spark.createDataFrame(data, schema)
result_df = df.groupBy("product").pivot("salesperson").sum("quality").orderBy("product")
display(result_df)
