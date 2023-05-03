#!/bin/bash

# print format log information
log_info(){
    echo -e "`date +%m-%d-%H:%M:%S`:\033[34m [info] \033[m $1"
}

cnt=0

# run ./timer_mysql2hive.pu every 30 minutes
while true
do
    log_info "start to run mysql2hive.py for $cnt times" >> $HOME/data_preprocess/mysql2hive/logs/mysql2hive.log 2>&1

    # run mysql2hive.py
    python3 $HOME/data_preprocess/mysql2hive/mysql2hive.py >> $HOME/data_preprocess/mysql2hive/logs/mysql2hive.log 2>&1

    # cnt + 1
    cnt=$[$cnt+1]

    # sleep 30 minutes
    sleep 1800
done



