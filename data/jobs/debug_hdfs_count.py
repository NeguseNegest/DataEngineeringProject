from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.option("header", True).csv("hdfs:///data/raw/telco/services")
print("ROWS:", df.count())
spark.stop()
