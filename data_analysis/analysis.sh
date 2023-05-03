#!/bin/bash

cnt=0

while true
do
    echo "start to run hive to event for $cnt times" >> $HOME/data_analysis/logs/analysis.log 2>&1

    # get file list of ./scripts and run them one by one in a for loop
    for file in `ls $HOME/data_analysis/scripts/*`
    do
        echo "start to run $file" >> $HOME/data_analysis/logs/hive2event.log 2>&1
        python3 $file >> $HOME/data_analysis/logs/hive2event.log 2>&1
        echo "finish to run $file" >> $HOME/data_analysis/logs/hive2event.log 2>&1

        sleep 60
    done

    # cnt + 1
    cnt=$[$cnt+1]

    # sleep 10 minutes
    sleep 600
done