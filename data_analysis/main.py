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



def getUserActivity():
    timetable_df = spark.sql("select * from default.timestable")
    maxtimestamp = timetable_df.select(F.max("timestamp_d").alias("timestamp_d"))
    timetable_df = timetable_df.select(timetable_df.time_hour, timetable_df.timestamp_d)
    timetable_df = timetable_df.join(maxtimestamp, maxtimestamp.timestamp_d==timetable_df.timestamp_d, 'inner')
    timetable_df = timetable_df.select(timetable_df.time_hour).limit(1)


    t_timetable_df = timetable_df.withColumn("id", F.lit(1))
    t_timetable_df = t_timetable_df.withColumnRenamed("time_hour", "created_at")
    # convert the time_hour to timestamp
    t_timetable_df = t_timetable_df.withColumn("created_at", F.to_timestamp(t_timetable_df.created_at, "yyyy-MM-dd-HH"))

    pushTable_df = spark.sql("select id as active_id,time,actor_id from default.pushTable")
    pushTable_df = pushTable_df.join(timetable_df, timetable_df.time_hour == pushTable_df.time).drop(timetable_df.time_hour).drop(pushTable_df.time)
    # print("==================================")
    # print(pushTable_df.filter(pushTable_df.active_id == "69688279").count())

    user = spark.sql("select id,login,company,followers,following,location,public_repos from default.users")
    # print("====================================")
    # print(user.select(user.location).distinct().collect())
    # print("====================================")

    user_rdd = user.rdd
    user_rdd = user_rdd.map(lambda x : (x[0], (x[1], x[2], x[3], x[4], x[5], x[6])))
    user_rdd = user_rdd.reduceByKey(lambda a, b: b)
    user_rdd = user_rdd.map(lambda x : (x[0], x[1][0], x[1][1], x[1][2], x[1][3], x[1][4], x[1][5]))

    user_df = spark.createDataFrame(user_rdd, ["id","login","company","followers","following","location","public_repos"])
    # print("====================================")
    # print(user_df.select(user_df.location).distinct().collect())
    # print("====================================")

    # mixed table contains many imformation to do more group on this dataframe
    activity_mix = user_df.join(pushTable_df, user_df.id == pushTable_df.actor_id).drop(pushTable_df.actor_id)
    activity_mix = activity_mix.distinct()



    # create a table contains the top 100 active users in this hour
    acivityGroupByUserid = activity_mix.groupBy('id').count()
    acivityGroupByUserid = acivityGroupByUserid.join(user_df, user_df.id == acivityGroupByUserid.id).drop(acivityGroupByUserid.id)
    topActiveUser = acivityGroupByUserid.filter(acivityGroupByUserid.public_repos > 0).filter(acivityGroupByUserid.public_repos != 0).orderBy("count", ascending=0)
    # rename count to activity_cnt
    topActiveUser = topActiveUser.withColumnRenamed("count", "activity_cnt")
    topActiveUser = topActiveUser.withColumn("updated_at", F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp"))
    topActiveUser = topActiveUser.withColumn("id", F.lit(1)).join(t_timetable_df, "id", "inner").drop("id")
    # topActiveUser.show(20)

    # define funtion to transform location to do better group
    def changelocation(s):
        s = s.strip()
        if "," in s:
            s = s.split(',')
            return s[-1].strip()
        return s

    # create a table contains the top 100 active location in this hour
    changelocationUDF = F.udf(changelocation, StringType())
    topActiveRegion = activity_mix.filter(activity_mix.location != "None")
    topActiveRegion = topActiveRegion.withColumn("location", changelocationUDF(topActiveRegion.location))
    topActiveRegion = topActiveRegion.groupBy('location').count().orderBy("count", ascending=0)
    topActiveRegion = topActiveRegion.withColumn("updated_at", F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp"))
    topActiveRegion = topActiveRegion.withColumnRenamed("count", "activity_cnt")
    # topActiveRegion.show(20)

    # create a table contains the top 100 active company in this hour
    topActiveCompany = activity_mix.filter(activity_mix.company != "None").groupBy('company').count().orderBy("count", ascending=0)
    topActiveCompany = topActiveCompany.withColumn("updated_at", F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp"))
    topActiveCompany = topActiveCompany.withColumnRenamed("count", "activity_cnt")
    # topActiveCompany.show(20)

    iodatabase(topActiveUser, "topActiveUser", "dws_userTopActiveUser", to_mongo=True, to_mysql=True, to_hive=True, show_count=False)
    iodatabase(topActiveRegion, "topActiveRegion", "dws_userTopActiveRegion", to_mongo=True, to_mysql=True, to_hive=True, show_count=False)
    iodatabase(topActiveCompany, "topActiveCompany", "dws_userTopActiveCompany", to_mongo=True, to_mysql=True, to_hive=True, show_count=False)



if __name__ == '__main__':
    # df = getEventAllCount()
    # iodatabase(df, "eventAllCount", "dws_eventCounts", to_mongo=False, to_mysql=False, to_hive=True, show_count=True)
    # df = getEventCount()
    # df.show()
    # iodatabase(df, "eventCount", "dws_eventCountsPerhour", to_mongo=False, to_mysql=False, to_hive=True, show_count=True)

    # getIssueBasic()
    # mysqlUserBasic()
    getUserActivity()

    
