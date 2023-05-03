from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, TimestampType
from pyspark.sql import HiveContext,SparkSession

from datetime import datetime, timedelta
import pandas as pd

import logging
from log import Logger

logger = Logger("/root/test-issue.log", logging.DEBUG, __name__).getlog()

mongodb_name = "results"
table_name = ["eventAllCount","eventAllRatio","eventCount","eventRatio","reposCount","usersCount"]
event_table_name = ["create","delete","issues","issueComment","pull","push","watch", "release"]

spark = SparkSession.builder\
    .appName("test")\
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse")\
    .config("spark.mongodb.input.uri", "mongodb://master02:37017")\
    .config("spark.mongodb.output.uri", "mongodb://master02:37017")\
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2")\
    .enableHiveSupport()\
    .getOrCreate()

hive_context = HiveContext(spark.sparkContext)

# input: df: dataframe, table: single table name
def iodatabase(wdf, table, htable, to_mongo=True, to_mysql=True, to_hive=True, show_count=True):
    if to_hive:
        # write data to hive
        # check if table exists
        try:
            wdf.createOrReplaceTempView("tmptable")
            s = spark.sql("show tables in default like '{}'".format(htable))
            flag = len(s.collect())
            if flag:
                logger.info("hive repo {} exist".format(htable))
                hive_context.sql("insert into default.{} select * from tmptable".format(htable))
            else:
                logger.info("hive repo {} not exist".format(htable))
                hive_context.sql("create table IF NOT EXISTS default.{} select * from tmptable".format(htable))
        except:
            logger.error("write data to hive's table {} failed".format(htable))
            pass
        


    if to_mongo:
        if show_count:  
            try:
                # read shcema from each mongodb collection
                rdf = spark.read.format("mongo").option("database", mongodb_name).option("collection", table).load()
                # count documents in each collection now
                logger.info("count of {} is {}".format(table, rdf.count()))
            except:
                logger.error("read data from mongodb's collections {} failed".format(table))
                pass

        # write data to mongodb's collections
        wdf.write.format("mongo").mode("append").option("database", mongodb_name).option("collection", table).save()
        logger.info("write data to mongodb's collections {} successfully".format(table))


    if to_mysql:
        if show_count:
            try:
                # read schema from mysql
                rdf = spark.read.format("jdbc").option(
                    url="jdbc:mysql://worker02:3306/github",
                    driver="com.mysql.cj.jdbc.Driver",
                    dbtable="(select count(*) from {}) as {}".format(table, table),
                    user="root",
                    password="123456"
                ).load()
                # count documents in each collection now
                logger.info("count of {} is {}".format(table, rdf.count()))
            except:
                logger.error("read data from mysql's table {} failed".format(table))
                pass

        # write data to mysql
        logger.info("write data to mysql's table {} successfully".format(table))

        wdf.write.format("jdbc")\
            .option("url", "jdbc:mysql://worker02:3306/github")\
            .option("driver", "com.mysql.cj.jdbc.Driver")\
            .option("dbtable", table)\
            .option("user", "root")\
            .option("password", "123456")\
            .mode("append")\
            .save()


def getIssueBasic():
    issue = spark.sql("select * from default.issue")

    # == get real data created time ==
    timetable_df = spark.sql("select * from default.timestable")
    maxtimestamp = timetable_df.select(F.max("timestamp_d").alias("timestamp_d"))
    timetable_df = timetable_df.select(timetable_df.time_hour, timetable_df.timestamp_d)
    timetable_df = timetable_df.join(maxtimestamp, maxtimestamp.timestamp_d==timetable_df.timestamp_d, 'inner')
    timetable_df = timetable_df.select(timetable_df.time_hour).limit(1)
    timetable_df = timetable_df.withColumn("id", F.lit(1)).withColumnRenamed("time_hour", "created_at")
    # convert the time_hour to timestamp
    timetable_df = timetable_df.withColumn("created_at", F.to_timestamp(timetable_df.created_at, "yyyy-MM-dd-HH"))

    # == Basic statistics ==
    statistics = {}
    
    # count number of state of issue
    state_open_num = issue.filter(issue.state == "open" ).count()
    statistics["issueOpenCount"] = state_open_num
    state_closed_num = issue.filter(issue.state == "closed").count()
    statistics["issueClosedCount"] = state_closed_num
    state_repoened_num = issue.filter(issue.state_reason == "reopened").count()
    statistics["issueReopenedCount"] = state_repoened_num
    completed_num = issue.filter(issue.state_reason == "completed").count()
    statistics["issueCompletedCount"] = completed_num
    other_closed_num = issue.filter(issue.state_reason != "completed").count()
    statistics["issueOtherClosedCount"] = other_closed_num

    # create spark dataframe
    statistics_df = spark.createDataFrame([statistics]).withColumn("id", F.lit(1)).join(timetable_df, "id", "inner").drop("id")
    statistics_df = statistics_df.withColumn("updated_at", F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp"))



    # == calculate the interval of closed issue by start time and end time ==
    interval = issue.filter(issue.state == "closed").select(F.datediff(issue.end_time, issue.start_time).alias("interval"))
    # get the max interval
    max_interval = interval.agg({"interval": "max"}).collect()[0][0]
    statistics["issueMaxInterval"] = max_interval
    # pivot to different interval level
    interval = interval.withColumn("interval", F.when(F.col("interval") <= 1, "I1").when(F.col("interval") <= 7, "I7").when(F.col("interval") <= 30, "I30").when(F.col("interval") <= 90, "I90").when(F.col("interval") <= 180, "I180").when(F.col("interval") <= 365, "I365").when(F.col("interval") <= 730, "I730").when(F.col("interval") <= 1095, "I1095").when(F.col("interval") <= 1460, "I1460").otherwise("others"))
    # get list of interval
    interval_list = interval.select("interval").distinct().rdd.flatMap(lambda x: x).collect()
    # count number of each interval
    interval_dict = {}
    for i in interval_list:
        interval_dict[i] = interval.filter(interval.interval == i).count()
    interval_df = spark.createDataFrame([interval_dict]).withColumn("updated_at", F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp"))
    interval_df = interval_df.withColumn("id", F.lit(1)).join(timetable_df, "id", "inner").drop("id")
    # interval_df.show()


    # == get statistics of different level of issue's comments ==
    # pivot to different comments number: 0, 1-10, 11-100, 101-1000, 1001-10000, 10001-100000, 100001-1000000, 1000001-10000000
    comments = issue.select(issue.comments).withColumn("comments", F.when(F.col("comments") == 0, "C0").when(F.col("comments") <= 10, "C10").when(F.col("comments") <= 100, "C100").when(F.col("comments") <= 1000, "C1000").when(F.col("comments") <= 10000, "C10000").when(F.col("comments") <= 100000, "C100000").when(F.col("comments") <= 1000000, "C1000000").otherwise("others"))
    comments_list = comments.select("comments").distinct().rdd.flatMap(lambda x: x).collect()
    comments_dict = {}
    for i in comments_list:
        comments_dict[i] = comments.filter(comments.comments == i).count()
    comments_df = spark.createDataFrame([comments_dict]).withColumn("updated_at", F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp"))
    comments_df = comments_df.withColumn("id", F.lit(1)).join(timetable_df, "id", "inner").drop("id")
    # comments_df.show()
    

    # == get statistics of different level of issue's number ==
    # pivot number to different level: 0, 1-10, 11-100, 101-1000, 1001-10000, 10001-100000, 100001-1000000, 1000001-10000000
    number = issue.select(issue.number).withColumn("number", F.when(F.col("number") == 0, "N0").when(F.col("number") <= 10, "N10").when(F.col("number") <= 100, "N100").when(F.col("number") <= 1000, "N1000").when(F.col("number") <= 10000, "N10000").when(F.col("number") <= 100000, "N100000").when(F.col("number") <= 1000000, "N1000000").otherwise("others"))
    number_list = number.select("number").distinct().rdd.flatMap(lambda x: x).collect()
    number_dict = {}
    for i in number_list:
        number_dict[i] = number.filter(number.number == i).count()
    number_df = spark.createDataFrame([number_dict]).withColumn("updated_at", F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp"))
    number_df = number_df.withColumn("id", F.lit(1)).join(timetable_df, "id", "inner").drop("id")
    # number_df.show()


    # write to database
    iodatabase(statistics_df, "issueBasic", "dws_issueBasic", to_mongo=True, to_mysql=True, to_hive=True, show_count=False)
    iodatabase(interval_df, "issueInterval", "dws_issueInterval", to_mongo=True, to_mysql=True, to_hive=True, show_count=False)
    iodatabase(comments_df, "issueComments", "dws_issueComments", to_mongo=True, to_mysql=True, to_hive=True, show_count=False)
    iodatabase(number_df, "issueNumber", "dws_issueNumber", to_mongo=True, to_mysql=True, to_hive=True, show_count=False)



if __name__ == '__main__':
    getIssueBasic()
    

    
