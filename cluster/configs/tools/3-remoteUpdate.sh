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
    log_info "ip: ${ipList[$index]}, node name: ${nodeList[$index]}, copy configs to remote node"
    rsync -avz -e "ssh -i $HOME/.ssh/id_rsa" $HOME/configs $userName@${ipList[$index]}:/$HOME/
    ssh -i "$HOME/.ssh/id_rsa" $userName@${ipList[$index]} "source ~/.bashrc; cat /dev/null > $HOME/configs/logs/update.log; bash $HOME/configs/tools/3-1-updateConfig.sh > $HOME/configs/logs/update.log"
done