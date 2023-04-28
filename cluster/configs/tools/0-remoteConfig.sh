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


# generate random strong password for temporary config
sudo apt install -y openssl
openssl rand -base64 40 > $HOME/configs/system/temp-passwd
initPassWd=`cat $HOME/configs/system/temp-passwd`
log_warn "current temp init password: $initPassWd"

# config ssh for all nodes
for index in ${!ipList[@]}
do
    log_info "ip: ${ipList[$index]}, node name: ${nodeList[$index]} config ssh"
    ssh -i "$HOME/configs/system/ali-5003.pem" $userName@${ipList[$index]} 'bash -s' < $HOME/configs/tools/0-1-sshConfig.sh $userName $initPassWd
done

# copy subdir of configs to all nodes by rsync
for index in ${!ipList[@]}
do
    log_info "ip: ${ipList[$index]}, node name: ${nodeList[$index]} copy configs"
    rsync -avz -e "ssh -i $HOME/configs/system/ali-5003.pem" $HOME/configs $userName@${ipList[$index]}:/$HOME/
done

# copy ssh key to all nodes by ssh-copy-id
for index in ${!ipList[@]}
do
    log_info "ip: ${ipList[$index]}, node name: ${nodeList[$index]} copy ssh key"
    ssh -i "$HOME/configs/system/ali-5003.pem" $userName@${ipList[$index]} 'bash -s' < $HOME/configs/tools/0-2-sshCopy.sh $userName $initPassWd
done

# [FIX:] adjust ssh config to avoid malicious shell script injection via port scanning and weak password attacks
## close password authentication, only permit key-pair login
sshSecurityInst="find /etc/ssh/ -name sshd_config | xargs perl -pi -e \"s|PasswordAuthentication yes|PasswordAuthentication no|g\"; sed -i '/StrictHostKeyChecking/c StrictHostKeyChecking yes' /etc/ssh/ssh_config; sed -i '/PermitRootLogin/c PermitRootLogin no' /etc/ssh/ssh_config; service ssh restart"
for index in ${!ipList[@]}
do
    log_info "ip: ${ipList[$index]}, node name: ${nodeList[$index]} remove password authentication"
    ssh -i "$HOME/.ssh/id_rsa" $userName@${ipList[$index]} "$sshSecurityInst"
done


# remote initialize all nodes by calling 1-initiate.sh
for index in ${!ipList[@]}
do
    log_info "ip: ${ipList[$index]}, node name: ${nodeList[$index]} remote initialize"
    # initialize mysql for master01
    if [ ${nodeList[$index]} == "master01" ]; then
        ./0-3-initialize.sh mysql > $HOME/configs/mysql/init.log
    fi
    # initialize all nodes for hadoop, spark, hive, hbase, kafka, zookeeper
    ## if directly initializing mysql for master01 in 0-3..sh, there will be timeout error
    ## whole script will exit early without finishing initialization of other nodes
    nohup ssh -i "$HOME/.ssh/id_rsa" $userName@${ipList[$index]} "source ~/.bashrc; sudo chmod a+x $HOME/configs/tools/*.sh; mkdir $HOME/configs/logs/; sudo nohup bash $HOME/configs/tools/0-3-initialize.sh > $HOME/configs/logs/init.log &" >/dev/null 2>&1 &
done
