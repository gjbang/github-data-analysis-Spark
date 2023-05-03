# This script write data to mongodb, and the data is from hive.
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import HiveContext

dbname = "test"
collectionname = "create"
mongourl = "mongodb://master02:37017/{}.{}".format(dbname, collectionname)


spark = SparkSession.builder\
    .appName("hive2mongodb")\
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse")\
    .config("spark.mongodb.input.uri", mongourl)\
    .config("spark.mongodb.output.uri", mongourl)\
    .config("spark.mongodb.output.database", dbname)\
    .config("spark.mongodb.output.collection", collectionname)\
    .config("spark.mongodb.output.mode", "append")\
    .config("spark.mongodb.output.replaceDocument", "false")\
    .enableHiveSupport()\
    .getOrCreate()


coll = spark.read.format("mongo").load()
coll.show()
print(type(coll))

df = spark.createDataFrame([{'_id': 'HIJKLMN', 'REQID': 'wafldwankl', 'EVENT': 'display'}])
df.write.format("mongo").mode("append").save()

