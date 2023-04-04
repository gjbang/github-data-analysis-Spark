export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export SPARK_MASTER_IP=master01
export SPARK_WORKER_MEMORY=1g
export SPARK_MASTER_PORT=7077
export MASTER=spark://master01:7077
export SPARK_MASTER_WEBUI_PORT=8090
export SPARK_WORKER_WEBUI_PORT=8091
export HADOOP_HOME=/opt/module/hadoop
export HADOOP_CONF_DIR=/opt/module/hadoop/etc/hadoop
export YARN_CONF_DIR=/opt/module/hadoop/etc/hadoop
# for job history - 18080
export SPARK_HISTORY_OPTS=-Dspark.history.provider=org.apache.spark.deploy.history.FsHistoryProvider
export SPARK_HISTORY_OPTS=-Dspark.history.fs.logDirectory=hdfs://master01:8020/user/hadoop/evtlogs


