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

read_df = hive_context.sql("select created_at from default.ods_activitytable limit 1")

def time_hour(time):
    times = time.split('T')
    hour  = times[1].split(':')[0]
    return times[0] + "-" + hour

def time_day(time):
    times = time.split('T')
    return times[0]

time_transformUDF = F.udf(time_hour, StringType())
time_dayUDF = F.udf(time_day, StringType())


inserts = read_df.withColumn("timestamp_d", F.lit(time.time()))
inserts = inserts.withColumn("time_hour", time_transformUDF(inserts.created_at))
inserts = inserts.withColumn("time_day", time_dayUDF(inserts.created_at))
inserts = inserts.drop('created_at')

inserts.registerTempTable('tmptable')
s = spark.sql("show tables in default like 'timesTable'")
flag = len(s.collect())
if flag:
    print("exist")
    spark.sql("insert into default.timesTable select * from tmptable")
else:
    print("not exist")
    spark.sql("create table IF NOT EXISTS default.timesTable select * from tmptable")
#
