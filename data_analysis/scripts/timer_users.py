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


def mysqlUserBasic():
    # read data from mysql
    rdf = spark.read.format("jdbc")\
            .option("url", "jdbc:mysql://worker02:3306/github")\
            .option("driver", "com.mysql.cj.jdbc.Driver")\
            .option("dbtable", "users")\
            .option("user", "root")\
            .option("password", "123456")\
            .load()

        # == get real data created time ==
    timetable_df = spark.sql("select * from default.timestable")
    maxtimestamp = timetable_df.select(F.max("timestamp_d").alias("timestamp_d"))
    timetable_df = timetable_df.select(timetable_df.time_hour, timetable_df.timestamp_d)
    timetable_df = timetable_df.join(maxtimestamp, maxtimestamp.timestamp_d==timetable_df.timestamp_d, 'inner')
    timetable_df = timetable_df.select(timetable_df.time_hour).limit(1)
    timetable_df = timetable_df.withColumn("id", F.lit(1)).withColumnRenamed("time_hour", "created_at")
    # convert the time_hour to timestamp
    timetable_df = timetable_df.withColumn("created_at", F.to_timestamp(timetable_df.created_at, "yyyy-MM-dd-HH"))



    statistics = {}

    # total user number
    user_num = rdf.count()
    # satisfy ratio of user having a public email
    statistics["email_ratio"] = rdf.filter(rdf.email != "null").count() / user_num * 100
    # satisfy ratio of user having a company
    statistics["company_ratio"] = rdf.filter(rdf.company != "null").count() / user_num * 100
    # satisfy ratio of user having a location
    statistics["location_ratio"] = rdf.filter(rdf.location != "null").count() / user_num * 100
    # satisfy ratio of user having a bio
    statistics["bio_ratio"] = rdf.filter(rdf.bio != "null").count() / user_num * 100
    # satisfy ratio of user having a blog
    statistics["blog_ratio"] = rdf.filter(rdf.blog != "null").count() / user_num * 100
    # satisfy ratio of user having a hireable
    statistics["hireable_ratio"] = rdf.filter(rdf.hireable != "null").count() / user_num * 100
    # satisfy ratio of user having a twitter_username
    statistics["twitter_username_ratio"] = rdf.filter(rdf.twitter_username != "null").count() / user_num * 100
    # satisfy ratio of user having a followers
    statistics["followers_ratio"] = rdf.filter(rdf.followers != 0).count() / user_num * 100
    # satisfy ratio of user having a following
    statistics["following_ratio"] = rdf.filter(rdf.following != 0).count() / user_num * 100
    # satisfy ratio of user having a positive ratio of followers/following
    statistics["followers_following_ratio"] = rdf.filter(rdf.followers / rdf.following > 1).count() / user_num * 100
    # satisfy ratio of user having a public_repo
    statistics["public_repos_ratio"] = rdf.filter(rdf.public_repos != 0).count() / user_num * 100
    # satisfy ratio of user having a public_gists
    statistics["public_gists_ratio"] = rdf.filter(rdf.public_gists != 0).count() / user_num * 100
    
    # cut float to only 2 float point
    for key in statistics:
        statistics[key] = round(statistics[key], 2)

    # create dataframe
    d = [statistics]
    ratio_df = spark.createDataFrame(d)
    ratio_df = ratio_df.withColumn("updated_at", F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp"))
    ratio_df = ratio_df.withColumn("id", F.lit(1)).join(timetable_df, "id", "inner").drop("id")

    # find the top 20 users who have the most followers
    top20_followers = rdf.orderBy(rdf.followers.desc()).select("id", "login", "followers").limit(20).withColumn("updated_at", F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp"))

    # find the top 20 users who have the most following
    top20_following = rdf.orderBy(rdf.following.desc()).select("id", "login", "following").limit(20).withColumn("updated_at", F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp"))

    iodatabase(ratio_df, "userBasic", "dws_userBasic", to_mongo=True, to_mysql=True, to_hive=True, show_count=False)
    iodatabase(top20_followers, "top20_followers", "dws_userTop20Followers", to_mongo=True, to_mysql=True, to_hive=True, show_count=False)
    iodatabase(top20_following, "top20_following", "dws_userTop20Following", to_mongo=True, to_mysql=True, to_hive=True, show_count=False)





if __name__ == '__main__':
    mysqlUserBasic()

    
