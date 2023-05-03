import sh
import time
from pyspark.sql import HiveContext,SparkSession

_SPARK_HOST = "spark://master01:7077"

spark = SparkSession.builder.master(_SPARK_HOST)\
    .appName("SparkOnHive - HDFS2Hive")\
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse")\
    .enableHiveSupport()\
    .getOrCreate()

# currentTime = time.strftime("%Y-%m-%d-%H", time.localtime())
# loadPath = "/testlog/gh_activity/"+ currentTime +"/*.json"
loadPath = "/testlog/gh_activity/*.json"
loadCommand = "load data inpath '" + loadPath + "' into table ods_activitytable"
spark.sql(loadCommand)