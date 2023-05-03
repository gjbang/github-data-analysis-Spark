from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, TimestampType
from pyspark.sql import HiveContext,SparkSession

from datetime import datetime, timedelta

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
            df.createOrReplaceTempView("tmptable")
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


def getIssueInterval():
    issue = spark.sql("select * from default.issue")

    # == Basic statistics ==

    # count number of state of issue
    state_num = issue.groupBy("state").count()

    # count the number of state reason of issue is closed
    state_reason_num = issue.filter(issue.state == "closed").groupBy("state", "state_reason").count()

    # count the state_reason reopen number of issue
    reopen_num = issue.filter(issue.state_reason == "reopened").count()
    # print("reopened_num: ", reopen_num)

    # merge the above into one dataframe
    state_num = state_num.withColumn("state_reason", F.lit(None).cast(StringType()))

    # group by comments and count the number of issue, convert comments to int
    comments_num = issue.withColumn("comments", F.col("comments").cast("int")).groupBy("comments").count().sort("comments", ascending=True).show()




# count the number of each events and merge into one record
def getEventAllCount():
    
    result = spark.createDataFrame([], StructType([]))
    # add id in the first column
    result = result.withColumn("id", F.lit(1))

    for event in event_table_name:
        event_df = spark.sql("select count(*) as {} from default.{}".format(event + "EventCount" ,event + "Table"))
        # add id in the first column
        event_df = event_df.withColumn("id", F.lit(1))
        # join the result dataframe
        result = result.join(event_df, "id", "outer")
        
    timetable_df = spark.sql("select * from default.timestable")
    maxtimestamp = timetable_df.select(F.max("timestamp_d").alias("timestamp_d"))
    timetable_df = timetable_df.select(timetable_df.time_hour, timetable_df.timestamp_d)
    timetable_df = timetable_df.join(maxtimestamp, maxtimestamp.timestamp_d==timetable_df.timestamp_d, 'inner')
    timetable_df = timetable_df.select(timetable_df.time_hour).limit(1)
    timetable_df = timetable_df.withColumn("id", F.lit(1)).withColumnRenamed("time_hour", "created_at")
    # convert the time_hour to timestamp
    timetable_df = timetable_df.withColumn("created_at", F.to_timestamp(timetable_df.created_at, "yyyy-MM-dd-HH"))

    result = result.join(timetable_df, "id", "outer")

    # add updated_at in the last column as timestamp
    result = result.withColumn("updated_at", F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp")).drop("id")
    return result




def getEventCount():
    timetable_df = spark.sql("select * from default.timestable")
    maxtimestamp = timetable_df.select(F.max("timestamp_d").alias("timestamp_d"))
    timetable_df = timetable_df.select(timetable_df.time_hour, timetable_df.timestamp_d)
    timetable_df = timetable_df.join(maxtimestamp, maxtimestamp.timestamp_d==timetable_df.timestamp_d, 'inner')
    timetable_df = timetable_df.select(timetable_df.time_hour).limit(1)

    createTable_df = spark.sql("select id,time from default.createTable")
    createTable_df = createTable_df.join(timetable_df, timetable_df.time_hour == createTable_df.time).count()
    delete_df = spark.sql("select id,time from default.deleteTable")
    delete_df = delete_df.join(timetable_df, timetable_df.time_hour == delete_df.time).count()
    issuesTable_df = spark.sql("select id,time from default.issuesTable")
    issuesTable_df = issuesTable_df.join(timetable_df, timetable_df.time_hour == issuesTable_df.time).count()
    issueCommentTable_df = spark.sql("select id,time from default.issueCommentTable")
    issueCommentTable_df = issueCommentTable_df.join(timetable_df, timetable_df.time_hour == issueCommentTable_df.time).count()
    pullTable_df = spark.sql("select id,time from default.pullTable")
    pullTable_df = pullTable_df.join(timetable_df, timetable_df.time_hour == pullTable_df.time).count()
    pushTable_df = spark.sql("select id,time from default.pushTable")
    pushTable_df = pushTable_df.join(timetable_df, timetable_df.time_hour == pushTable_df.time).count()
    releaseTable_df = spark.sql("select id,time from default.releaseTable")
    releaseTable_df = releaseTable_df.join(timetable_df, timetable_df.time_hour == releaseTable_df.time).count()
    watchTable_df = spark.sql("select id,time from default.watchTable")
    watchTable_df = watchTable_df.join(timetable_df, timetable_df.time_hour == watchTable_df.time).count()

    d = [{'createEventCount': createTable_df, 'deleteEventCount': delete_df, 'issuesEventCount': issuesTable_df, 'issueCommentEventCount': issueCommentTable_df, 'pullEventCount': pullTable_df, 'pushEventCount': pushTable_df, 'releaseEventCount': releaseTable_df, 'watchEventCount':watchTable_df}]
    eventCount_df = spark.createDataFrame(d)
    timetable_df = timetable_df.withColumn("createEventCount", F.lit(createTable_df))
    eventCount_df = eventCount_df.join(timetable_df, timetable_df.createEventCount == eventCount_df.createEventCount).drop(timetable_df.createEventCount)
    eventCount_df = eventCount_df.withColumn("created_at", F.to_timestamp(eventCount_df.time_hour, "yyyy-MM-dd-HH")).drop("time_hour")
    eventCount_df = eventCount_df.withColumn("updated_at", F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp"))

    return eventCount_df


if __name__ == '__main__':
    df = getEventAllCount()
    iodatabase(df, "eventAllCount", "dws_eventCounts", to_mongo=True, to_mysql=True, to_hive=True, show_count=False)
    df = getEventCount()
    iodatabase(df, "eventCount", "dws_eventCountsPerhour", to_mongo=True, to_mysql=True, to_hive=True, show_count=False)