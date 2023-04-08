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

initPassWd="heikediguo"


# read lines from hosts and split by space
# get all nodes name
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

# config ssh for all nodes
for index in ${!ipList[@]}
do
    log_info "ip: ${ipList[$index]}, node name: ${nodeList[$index]} config ssh"
    ssh -i "$HOME/configs/system/ali-5003.pem" root@${ipList[$index]} < $HOME/configs/tools/0-1-sshConfig.sh
done

# copy subdir of configs to all nodes by rsync
for index in ${!ipList[@]}
do
    log_info "ip: ${ipList[$index]}, node name: ${nodeList[$index]} copy configs"
    rsync -avz -e "ssh -i $HOME/configs/system/ali-5003.pem" $HOME/configs root@${ipList[$index]}:/root/
done

# copy ssh key to all nodes by ssh-copy-id
for index in ${!ipList[@]}
do
    log_info "ip: ${ipList[$index]}, node name: ${nodeList[$index]} copy ssh key"
    ssh -i "$HOME/configs/system/ali-5003.pem" root@${ipList[$index]} < $HOME/configs/tools/0-2-sshCopy.sh
done

# remote initialize all nodes by calling 1-initiate.sh
for index in ${!ipList[@]}
do
    log_info "ip: ${ipList[$index]}, node name: ${nodeList[$index]} remote initialize"
    ssh -i "$HOME/.ssh/id_rsa" root@${ipList[$index]} "source ~/.bashrc; sudo chmod a+x $HOME/configs/tools/*.sh; mkdir $HOME/configs/logs/; bash $HOME/configs/tools/0-1-initialize.sh > $HOME/configs/logs/init.log" &
done