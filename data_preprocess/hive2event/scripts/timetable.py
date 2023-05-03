import time
import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql import HiveContext,SparkSession

_SPARK_HOST = "spark://master01:7077"

spark = SparkSession.builder.master(_SPARK_HOST)\
    .appName("SparkOnHive")\
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse")\
    .enableHiveSupport()\
    .getOrCreate()

hive_context = HiveContext(spark)

def get_time(hours):
    currentTime = (datetime.datetime.now() - datetime.timedelta(hours=hours)).strftime("%Y-%m-%d-%H-%A")
    print(currentTime)
    times = currentTime.split("-")
    year = times[0]
    month = times[1]
    day = times[2]
    hour = times[3]
    week = times[4]
    if week == "Sunday" or week == "Saturday":
        rest = "rest"
    else:
        rest = "not-rest"
    if int(hour) > 7 and int(hour) < 18:
        daytime = "day_time"
    else:
        daytime = "night_time"
    return year, month, day, hour, week, rest, daytime

l1 = "asia"
y1, m1, d1, h1, w1, r1, n1 = get_time(0)
l2 = "europe"
y2, m2, d2, h2, w2, r2, n2 = get_time(-4)
l3 = "america"
y3, m3, d3, h3, w3, r3, n3 = get_time(-12)


times = [(l1, y1, m1, d1, h1, w1, r1, n1),
          (l2, y2, m2, d2, h2, w2, r2, n2),
          (l3, y3, m3, d3, h3, w3, r3, n3)]

print(times)
schema = ['location', 'year', 'month', 'day', 'hour', 'week', 'rest', 'daytime']
inserts = spark.createDataFrame(times, schema)


inserts.registerTempTable('tmptable')
s = spark.sql("show tables in default like 'timeTable'")
flag = len(s.collect())
if flag:
    print("exist")
    spark.sql("insert into default.timeTable select * from tmptable")
else:
    print("not exist")
    spark.sql("create table IF NOT EXISTS default.timeTable select * from tmptable")
#
