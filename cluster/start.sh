#!/bin/bash

configPath="/opt/module"
serverName=`hostname`

# print format log information
log_info(){
    echo -e "`date +%m-%d-%H:%M:%S`:\033[34m [info] \033[m $1"
}

log_debug(){
    echo -e "`date +%m-%d-%H:%M:%S`:\033[32m [debug] \033[m $1"
}

log_warn(){
    echo -e "`date +%m-%d-%H:%M:%S`:\033[33m [warning] \033[m $1" 
}

log_error(){
    echo -e "`date +%m-%d-%H:%M:%S`:\033[31m [error] \033[m $1"
}


# run ssh serivce
log_info "start ssh service"
service ssh start


log_info "start master service"

# start hadoop
log_info "start hadoop service"
$configPath/hadoop/sbin/start-dfs.sh
$configPath/hadoop/sbin/start-yarn.sh
$configPath/hadoop/bin/mapred --daemon start historyserver

# # start zookeeper
# log_info "start zookeeper service"
# $configPath/zookeeper/bin/zkServer.sh start

# start kafka
log_info "start kafka service"
$configPath/kafka/bin/kafka-server-start.sh -daemon $configPath/kafka/config/server.properties

# start flume
log_info "start flume service"
$configPath/flume/bin/flume-ng agent -n a1 -c $configPath/flume/conf -f $configPath/flume/conf/flume.conf -Dflume.root.logger=INFO,console

# # start hive
# log_info "start hive service"
# $configPath/hive/bin/hive --service metastore &
# $configPath/hive/bin/hive --service hiveserver2 &

# # start spark
# log_info "start spark service"
# $configPath/spark/sbin/start-all.sh


