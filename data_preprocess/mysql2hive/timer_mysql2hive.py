import os
import time
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler

from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.types import *


def import_mysql2hive():
    # .config('spark.executor.extraClassPath', '/opt/module/spark/jars/*')\
    # .config('spark.driver.extraClassPath', '/opt/module/spark/jars/*')\
    # .config("spark.jars", "mysql-connector-java-8.0.24.jar")\
    spark = SparkSession.builder\
        .appName("mysql2hive")\
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse")\
        .enableHiveSupport()\
        .getOrCreate()


    # user_mysql_df = spark.read.format("jdbc").options(
    #     url="jdbc:mysql://localhost:3306/github",
    #     driver="com.mysql.jdbc.Driver",
    #     dbtable="users",
    #     user="roor",
    #     password="heikediguo"
    # ).load()

    # attributes required by user dim table
    # users_attr = "login, id, node_id, gravatar_id, type,name, company, blog, location, email, hireable, bio, twitter_username, public_repos, public_gists, followers, following, created_at, updated_at"

    LOCAL_FORMAT = "%Y-%m-%d %H:%M:%S"
    UTC_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

    # check if log file exists or create it
    if not os.path.exists("./logs/users_latest.log"):
        os.system("touch ./logs/users_latest.log")

    if not os.path.exists("./logs/repos_latest.log"):
        os.system("touch ./logs/repos_latest.log")

    # read latest datetime timestamp from log file
    with open("./logs/users_latest.log", "r") as f:
        users_latest = f.read()
        if users_latest == "":
            users_latest = datetime.strptime("1970-01-01 00:00:00", LOCAL_FORMAT).strftime(UTC_FORMAT)
        else:
            users_latest = datetime.strptime(users_latest, LOCAL_FORMAT).strftime(UTC_FORMAT)

    with open("./logs/repos_latest.log", "r") as f:
        repos_latest = f.read()
        if repos_latest == "":
            repos_latest = datetime.strptime("1970-01-01 00:00:00", LOCAL_FORMAT).strftime(UTC_FORMAT)
        else:
            repos_latest = datetime.strptime(repos_latest, LOCAL_FORMAT).strftime(UTC_FORMAT)


    # read mysql data which updated after latest datetime timestamp
    user_mysql_df = spark.read.format("jdbc").options(
        url="jdbc:mysql://worker02:3306/github",
        driver="com.mysql.cj.jdbc.Driver",
        dbtable="(select * from `users` where updated_at > '{}') as tusers".format(users_latest),
        user="root",
        password="123456"
    ).load()


    spark.sql("use default")
    s = spark.sql("show tables in default like 'users'")
    user_mysql_df.registerTempTable("users")

    if len(s.collect()):
        spark.sql("insert into default.users select * from users")
    else:
        # # create if not exists table
        spark.sql("create table IF NOT EXISTS default.users select * from users")

    # update latest datetime timestamp with user_mysql_df' latest datetime timestamp
    with open("./logs/users_latest.log", "w") as f:
        f.write(str(user_mysql_df.agg({"updated_at": "max"}).collect()[0][0]))


    # read mysql data
    repos_mysql_df = spark.read.format("jdbc").options(
        url="jdbc:mysql://worker02:3306/github",
        driver="com.mysql.cj.jdbc.Driver",
        dbtable="(select * from `repos` where updated_at > '{}') as trepos".format(repos_latest),
        user="root",
        password="123456"
    ).load()

    spark.sql("use default")
    s = spark.sql("show tables in default like 'repos'")
    repos_mysql_df.registerTempTable("repos")

    if len(s.collect()):
        spark.sql("insert into default.repos select * from repos")
    else:
        # # create if not exists table
        spark.sql("create table IF NOT EXISTS default.repos select * from repos")

    # update latest datetime timestamp with repos_mysql_df' latest datetime timestamp
    with open("./logs/repos_latest.log", "w") as f:
        f.write(str(repos_mysql_df.agg({"updated_at": "max"}).collect()[0][0]))


    # write log to record the number of rows inserted
    with open("./logs/timer_mysql2hive.log", "a") as f:
        f.write("==== {} ====\n".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        f.write("users: {}\n".format(user_mysql_df.count()))
        f.write("repos: {}\n".format(repos_mysql_df.count()))

    # close spark session
    spark.stop()

    with open("./logs/timer_mysql2hive.log", "a") as f:
        f.write("=== finish current round ===\n")


if __name__ == '__main__':
    # start immediately
    import_mysql2hive()

    # # start timer
    # scheduler = BackgroundScheduler()
    # scheduler.add_job(import_mysql2hive, 'interval', minutes=30)


    # try:
    #     scheduler.start()
    # except (KeyboardInterrupt, SystemExit):
    #     scheduler.shutdown()

    # while True:
    #     time.sleep(1800)
    #     # write log
    #     with open("./logs/timer_mysql2hive.log", "a") as f:
    #         f.write("==== Waiting for next round ====\n")