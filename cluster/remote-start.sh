#!/bin/bash

ipList=()
nodeList=()


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
done < $HOME/configs/hosts

for index in ${!ipList[@]}
do
    log_info "ip: ${ipList[$index]}, node name: ${nodeList[$index]}"

    ssh -i "$HOME/configs/ali-5003.pem" root@${ipList[$index]} "source ~/.bashrc; bash $HOME/start.sh "
done