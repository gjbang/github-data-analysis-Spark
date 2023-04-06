#!/bin/bash

# ======== Attention ========
# This script needs to be executed 
# after all nodes have executed init.sh
# which means all nodes have a password to login

configPath="/opt/module"
serverName=`hostname`

nodeList=()
initPassWd="heikediguo"


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


# read lines from hosts and split by space
while read line
do
    # split by space
    arr=($line)
    # get server name
    nodeName=${arr[1]}

    # add node name 
    # if [ $serverName != "master01" ]; then
    nodeList[${#nodeList[@]}]=$nodeName
    # fi

    log_info "inspect node name: $nodeName"
done < $HOME/configs/hosts

for node in ${nodeList[@]}
do
    if [ $serverName != $node ]; then
        log_info "start to config $node"
        sshpass -p $initPassWd ssh-copy-id -i ~/.ssh/id_rsa.pub root@$node 
    fi

    if [ $node == "master01" ]; then
        log_info "start init hive schema"
        $configPath/hive/bin/schematool -dbType mysql -initSchema -verbose
    fi
done