# config:utf8

from pyspark import SparkContext, SparkConf

if __name__ == '__main__':
    conf = SparkConf().setAppName("Word Count").setMaster("local[*]")
    sc = SparkContext(conf= conf)

    # 1. read file
    rdd = sc.textFile("file:///root/test/data/words.txt")

    # 2. split
    rdd = rdd.flatMap(lambda line:line.split(" "))

    # 3. construct tuple
    rdd = rdd.map(lambda x: (x,1))

    # # 4. groupby
    # rdd = rdd.reduceByKey(lambda a,b: a+b)
    # # 5.print
    # print(rdd.collect())

    print(rdd.countByKey())