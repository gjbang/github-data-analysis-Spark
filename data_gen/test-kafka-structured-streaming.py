# use pyspark struct streaming to read data from kafka
# the kafka topic is "gh_activity"
# the data is json format from github activity api

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("test-kafka")\
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.2')\
    .config('spark.jars.packages', 'org.apache.spark:kafka-clients-3.4.0.jar')\
    .config("spark.driver.memory", "3G")\
    .config("spark.driver.maxResultSize", "3G")\
    .getOrCreate()

# read data from kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "master01:9092") \
    .option("subscribe", "gh_activity") \
    .option("startingOffsets", "earliest") \
    .load()

print("finish load()")

# convert binary to string
df1 = df.selectExpr("CAST(value AS STRING)")

print("finish select expr")

# apply word count
df2 = df1.select(explode(split(df1.value, " ")).alias("word"))
df3 = df2.groupBy("word").count()

# write to console
query = df3.writeStream.outputMode("complete").format("console").start()

query.awaitTermination(60)
query.stop()
