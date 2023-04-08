#!/bin/bash

filePath=$HOME/configs
configPath=/opt/module

declare -a moduleDict
moduleDict=(
    ['flume']="conf"
    ['hadoop']="etc/hadoop"
    ['hbase']="conf"
    ['hive']="conf"
    ['kafka']="config"
    ['spark']="conf"
    ['zookeeper']="conf"
    )

nodeList=()

while read line
do
    nodeName=${arr[1]}
    nodeList[${#nodeList[@]}]=$nodeName
done < $HOME/configs/hosts

for node in ${nodeList[@]}
do
    for module in ${moduleList[@]}
    do
        rsync -anv $filePath/$module/ $node:$configPath/$module/${moduleList[$module]}
    done

done
