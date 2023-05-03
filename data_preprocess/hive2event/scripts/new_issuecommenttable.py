from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql import HiveContext,SparkSession
import json
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
def issue_id(s):
    map = json.loads(s)
    return map['id']
get_issue_idUDF = F.udf(issue_id, StringType())
time_transformUDF = F.udf(time_transform, StringType())

inserts = read_df.filter("type = 'IssueCommentEvent'")
inserts = inserts.withColumn("actor_id", inserts.actor["id"])
inserts = inserts.withColumn("repo_id", inserts.repo["id"])
inserts = inserts.withColumn("payload_action", inserts.payload["action"])
inserts = inserts.withColumn("payload_issue_id", get_issue_idUDF(inserts.payload["issue"]))
inserts = inserts.withColumn("time", time_transformUDF(inserts.created_at))
inserts = inserts.drop('actor', 'repo', 'payload', 'created_at', 'public')
inserts.registerTempTable('tmptable')

s = spark.sql("show tables in default like 'issueCommentTable'")
flag = len(s.collect())
if flag:
    print("exist")
    spark.sql("insert into default.issueCommentTable select * from tmptable")
else:
    print("not exist")
    spark.sql("create table IF NOT EXISTS default.issueCommentTable select * from tmptable")

