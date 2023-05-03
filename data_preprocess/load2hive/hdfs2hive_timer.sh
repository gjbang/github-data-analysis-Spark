#!/bin/bash

# print format log information
log_info(){
    echo -e "`date +%m-%d-%H:%M:%S`:\033[34m [info] \033[m $1"
}

cnt=0

while true
do
    log_info "start to run load2hive.py for $cnt times" >> $HOME/data_preprocess/load2hive/logs/load2hive.log 2>&1

    # run hdfs2hive.py
    python3 $HOME/data_preprocess/load2hive/load2hive.py >> $HOME/data_preprocess/load2hive/logs/load2hive.log 2>&1

    # cnt + 1
    cnt=$[$cnt+1]

    # sleep 10 minutes
    sleep 600
done