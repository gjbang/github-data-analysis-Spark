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
initPassWd=$1


# copy ssh key to all nodes
log_info "start to copy ssh key to all nodes"
# read lines from hosts and split by space
while read line
do
    # split by space
    arr=($line)
    # get ip
    ip=${arr[0]}
    # get node name
    nodeName=${arr[1]}
    # add ip to ipList
    ipList+=($ip)
    # add node name to nodeList
    nodeList+=($nodeName)

    log_info "inspect node name: $nodeName"
done < $HOME/configs/system/hosts

for index in ${!ipList[@]}
do
    log_info "ip: ${ipList[$index]}, node name: ${nodeList[$index]}"
    sshpass -p $initPassWd ssh-copy-id -i ~/.ssh/id_rsa.pub root@${ipList[$index]}
done