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

time_transformUDF = F.udf(time_transform, StringType())

inserts = read_df.filter("type = 'IssuesEvent'")
inserts = inserts.withColumn("actor_id", inserts.actor["id"])
inserts = inserts.withColumn("repo_id", inserts.repo["id"])
inserts = inserts.withColumn("payload_action", inserts.payload["action"])


def get_state_reason(s):
    map = json.loads(s)
    return map['state_reason']
def get_state(s):
    map = json.loads(s)
    return map['state']
def get_state_comment(s):
    map = json.loads(s)
    return map['comments']
def get_number(s):
    map = json.loads(s)
    return map['number']
def start_time(s):
    map = json.loads(s)
    return map['created_at']
def end_time(s):
    map = json.loads(s)
    return map['closed_at']
def issue_id(s):
    map = json.loads(s)
    return map['id']
get_state_reasonUDF = F.udf(get_state_reason, StringType())
get_stateUDF = F.udf(get_state, StringType())
get_state_commentUDF = F.udf(get_state_comment, StringType())
get_numberUDF = F.udf(get_number, StringType())
get_start_timeUDF = F.udf(start_time, StringType())
get_end_timeUDF = F.udf(end_time, StringType())
get_issue_idUDF = F.udf(issue_id, StringType())
inserts = inserts.withColumn("payload_issue_id", get_issue_idUDF(inserts.payload["issue"]))
inserts = inserts.withColumn("time", time_transformUDF(inserts.created_at))

inserts2 = inserts.drop('id')
inserts2 = inserts2.withColumn("id", get_issue_idUDF(inserts.payload["issue"]))
inserts2 = inserts2.withColumn("state_reason", get_state_reasonUDF(inserts.payload["issue"]))
inserts2 = inserts2.withColumn("state", get_stateUDF(inserts.payload["issue"]))
inserts2 = inserts2.withColumn("comments", get_state_commentUDF(inserts.payload["issue"]))
inserts2 = inserts2.withColumn("number", get_numberUDF(inserts.payload["issue"]))
inserts2 = inserts2.withColumn("start_time", get_start_timeUDF(inserts.payload["issue"]))
inserts2 = inserts2.withColumn("end_time", get_end_timeUDF(inserts.payload["issue"]))
inserts2 = inserts2.drop('payload_issue_id', 'actor', 'repo', 'payload', 'actor_id', 'created_at', 'public', 'repo_id', 'payload_action', 'type', 'time')

inserts = inserts.drop('actor', 'repo', 'payload', 'created_at', 'public')

inserts.registerTempTable('tmptable')

s = spark.sql("show tables in default like 'issuesTable'")
flag = len(s.collect())
if flag:
    print("exist")
    spark.sql("insert into default.issuesTable select * from tmptable")
else:
    print("not exist")
    spark.sql("create table IF NOT EXISTS default.issuesTable select * from tmptable")

inserts2.registerTempTable('tmptable2')
s2 = spark.sql("show tables in default like 'issue'")
flag2 = len(s2.collect())
if flag2:
    print("exist")
    spark.sql("insert into default.issue select * from tmptable2")
else:
    print("not exist")
    spark.sql("create table IF NOT EXISTS default.issue select * from tmptable2")