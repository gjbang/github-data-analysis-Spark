#!/bin/bash

configPath="/opt/module"
serverName=`hostname`
HIVE_LOG_DIR=$configPath/hive/logs

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


# stop hbase, hive, spark, kafka, flume, hadoop, zookeeper
log_warn "stop hbase service"
$configPath/hbase/bin/stop-hbase.sh

log_warn "stop hive service"
$configPath/hive/bin/hive --service metastore &>/dev/null
$configPath/hive/bin/hive --service hiveserver2 &>/dev/null

log_warn "stop spark service"
$configPath/spark/sbin/stop-all.sh

log_warn "stop kafka service"
$configPath/kafka/bin/kafka-server-stop.sh

# need to set configuration name to replace "flume"
log_warn "stop flume service"
ps -ef | grep test-1 | grep -v grep | awk '{print $2}' | xargs kill -9

log_warn "stop hadoop service"
$configPath/hadoop/sbin/stop-yarn.sh
$configPath/hadoop/sbin/stop-dfs.sh

log_warn "stop zookeeper service"
$configPath/zookeeper/bin/zkServer.sh stop

# # stop mysql
# log_info "stop mysql service"
# service mysqld stop
