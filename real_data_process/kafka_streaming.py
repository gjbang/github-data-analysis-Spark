# use pyspark struct streaming to read data from kafka
# the kafka topic is "gh_activity"
# the data is json format from github activity api

import json
import os

memory = '2g'
pyspark_submit_args = ' --driver-memory ' + memory + ' pyspark-shell'
os.environ["PYSPARK_SUBMIT_ARGS"] = pyspark_submit_args

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from pyspark.streaming import StreamingContext

# need to add local jar package

spark = SparkSession.builder.appName("test-kafka")\
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2')\
    .getOrCreate()

# read data from kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "worker02:9092") \
    .option("subscribe", "gh_activity") \
    .option("startingOffsets", "latest") \
    .option("cleanSource", "delete") \
    .option("failOnDataLoss", "false") \
    .load()

# use watermark to write with append mode
df = df.withWatermark("timestamp", "1 minutes")


# write to mysql by foreachBatch
def write_to_mysql(df, epoch_id):
    df.write.format("jdbc")\
        .option("url", "jdbc:mysql://worker02:3306/github")\
        .option("driver", "com.mysql.cj.jdbc.Driver")\
        .option("dbtable", "realStreamEventCnt")\
        .option("user", "root")\
        .option("password", "123456")\
        .mode("append")\
        .save()



# schema for github open api json
schema = StructType([
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("actor", StructType([
        StructField("id", LongType(), True),
        StructField("login", StringType(), True),
    ]), True),
    StructField("repo", StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("url", StringType(), True)
    ]), True),
    StructField("payload", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("org", StringType(), True),
])

# convert binary to string
df = df.selectExpr("CAST(value AS STRING)")

# if df is not empty, then process
# if df is empty, then skip
# cast from string to json
df = df.select(from_json(df.value, schema).alias("data"))
# divide the repo to multiple columns and rename with prefix corresponding to the original column
df = df.select("data.*", col("data.repo.id").alias("repo_id"), col("data.repo.name").alias("repo_name"), col("data.repo.url").alias("repo_url"))
# remove the original repo column
df = df.drop("repo").drop("actor").drop("org").drop("payload").drop("created_at")


# == Basic Statistics ==
# count of each type
df = df.groupBy(col("type")).count()
# add updated_at timestamp
df = df.withColumn("updated_at", current_timestamp())



# output to mysql with watermark
query = df.withWatermark("updated_at", "1 minutes").writeStream.outputMode("append").trigger(processingTime='1 minutes').foreachBatch(write_to_mysql).start()
# query = df.writeStream.outputMode("append").trigger(processingTime='1 minutes').foreachBatch(write_to_mysql).start()

query.awaitTermination()
query.stop()
