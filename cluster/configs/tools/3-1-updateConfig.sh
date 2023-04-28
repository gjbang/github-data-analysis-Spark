#!/bin/bash

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


ipList=()
nodeList=()
userName="root"
configPath="/opt/module"

# copy new local config files to conf dir
# for spark, hive, hadoop, hbase, zookeeper, kadka, flume, mysql

# spark
log_info "copy spark config files"
cp $HOME/configs/spark/* $configPath/spark/conf/

# hive
log_info "copy hive config files"
cp $HOME/configs/hive/* $configPath/hive/conf/

# hadoop
log_info "copy hadoop config files"
cp $HOME/configs/hadoop/* $configPath/hadoop/etc/hadoop/

# hbase
# log_info "copy hbase config files"
# cp $HOME/configs/hbase/* $configPath/hbase/conf/

# zookeeper
log_info "copy zookeeper config files"
cp $HOME/configs/zookeeper/zoo.cfg $configPath/zookeeper/conf/zoo.cfg
cp $HOME/configs/zookeeper/myid $configPath/zookeeper/zkData/myid

# kafka
# log_info "copy kafka config files"
# cp $HOME/configs/kafka/server.properties $configPath/kafka/config/server.properties
# # ! change broker.id ---- vim /opt/module/kafka/config/server.properties
# find $configPath/kafka/config/ -name "server.properties" | xargs perl -pi -e "s|broker.id=0|${kafkaBlockId[$serverName]}|g"

# flume
log_info "copy flume config files"
cp $HOME/configs/flume/flume-env.sh $configPath/flume/conf/flume-env.sh

# # mysql
# log_info "copy mysql config files"
# cp $HOME/configs/mysql/* $configPath/mysql/etc/
