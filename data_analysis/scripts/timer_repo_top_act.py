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
            .mode("overwrite")\
            .save()




def getRepoTopActivity():
    timetable_df = spark.sql("select * from default.timestable")
    maxtimestamp = timetable_df.select(F.max("timestamp_d").alias("timestamp_d"))
    timetable_df = timetable_df.select(timetable_df.time_hour, timetable_df.timestamp_d)
    timetable_df = timetable_df.join(maxtimestamp, maxtimestamp.timestamp_d==timetable_df.timestamp_d, 'inner')
    timetable_df = timetable_df.select(timetable_df.time_hour).limit(1)


    t_timetable_df = timetable_df.withColumn("nid", F.lit(1))
    t_timetable_df = t_timetable_df.withColumnRenamed("time_hour", "created_at")
    # convert the time_hour to timestamp
    t_timetable_df = t_timetable_df.withColumn("created_at", F.to_timestamp(t_timetable_df.created_at, "yyyy-MM-dd-HH"))

    pushTable_df = spark.sql("select id as event_id,time,actor_id, repo_id from default.pushTable")
    pushTable_df = pushTable_df.join(timetable_df, timetable_df.time_hour == pushTable_df.time).drop(timetable_df.time_hour).drop(pushTable_df.time)

    repo = spark.sql("select id, name, full_name, language from default.repos")
    repo_rdd = repo.rdd
    repo_rdd = repo_rdd.map(lambda x : (x[0], (x[1], x[2], x[3])))
    repo_rdd = repo_rdd.reduceByKey(lambda a, b: b)
    repo_rdd = repo_rdd.map(lambda x : (x[0], x[1][0], x[1][1], x[1][2]))
    repo_df = spark.createDataFrame(repo_rdd, ["id", "name", "full_name", "language"])

    push_repo = pushTable_df.join(repo_df, pushTable_df.repo_id == repo_df.id).drop(repo_df.id)
    push_repo = push_repo.distinct()

    # top active repos per hour
    topActiveRepo = push_repo.groupBy(push_repo.repo_id).count()
    topActiveRepo = topActiveRepo.join(repo, repo.id == topActiveRepo.repo_id).orderBy("count", ascending=0)
    topActiveRepo = topActiveRepo.withColumnRenamed("count", "activity_cnt")
    topActiveRepo = topActiveRepo.withColumnRenamed("id", "act_id")
    topActiveRepo = topActiveRepo.withColumn("updated_at", F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp"))
    topActiveRepo = topActiveRepo.withColumn("nid", F.lit(1)).join(t_timetable_df, "nid", "inner").drop("nid")
    # topActiveRepo.show(20)


    # top languages of active repos per hour
    ###############################################
    ### 根据语言的使用数量， 可以预测语言的变化趋势 ######
    ###############################################
    topLanguage = push_repo.filter(push_repo.language != "None").groupBy('language').count().orderBy("count", ascending=0)
    topLanguage = topLanguage.withColumnRenamed("count", "activity_cnt")
    topLanguage = topLanguage.withColumn("updated_at", F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp"))
    topLanguage = topLanguage.withColumn("nid", F.lit(1)).join(t_timetable_df, "nid", "inner").drop("nid")
    # topLanguage.show(20)

    iodatabase(topActiveRepo, "topActiveRepo", "dws_repoTopActiveRepo", to_mongo=True, to_mysql=True, to_hive=True, show_count=False)
    iodatabase(topLanguage, "topLanguage", "dws_repoTopLanguage", to_mongo=True, to_mysql=True, to_hive=True, show_count=False)


if __name__ == '__main__':

    getRepoTopActivity()
    
