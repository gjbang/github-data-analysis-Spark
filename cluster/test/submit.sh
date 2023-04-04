#!/bin/bash

# test spark-submit
spark-submit --master spark://master01:7077 --class org.apache.spark.examples.SparkPi /opt/module/spark/examples/jars/spark-examples_2.12-3.3.2.jar 3000

# test yarn-submit
spark-submit --master yarn --deploy-mode client --class org.apache.spark.examples.SparkPi /opt/module/spark/examples/jars/spark-examples_2.12-3.3.2.jar 3000
