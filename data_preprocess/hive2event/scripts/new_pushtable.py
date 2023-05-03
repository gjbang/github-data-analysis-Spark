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
read_df = hive_context.sql("select * from default.ods_activitytable")

def time_transform(time):
    times = time.split('T')
    hour  = times[1].split(':')[0]
    return times[0] + "-" + hour

time_transformUDF = F.udf(time_transform, StringType())

inserts = read_df.filter("type = 'PushEvent'")
inserts = inserts.withColumn("actor_id", inserts.actor["id"])
inserts = inserts.withColumn("repo_id", inserts.repo["id"])
inserts = inserts.withColumn("payload_push_id", inserts.payload["push_id"])
inserts = inserts.withColumn("payload_size", inserts.payload["size"])
inserts = inserts.withColumn("payload_distinct_size", inserts.payload["distinct_size"])
inserts = inserts.withColumn("time", time_transformUDF(inserts.created_at))
inserts = inserts.drop('actor', 'repo', 'payload', 'created_at', 'public')
inserts.registerTempTable('tmptable')

s = spark.sql("show tables in default like 'pushTable'")
flag = len(s.collect())
if flag:
    print("exist")
    spark.sql("insert into default.pushTable select * from tmptable")
else:
    print("not exist")
    spark.sql("create table IF NOT EXISTS default.pushTable select * from tmptable")

