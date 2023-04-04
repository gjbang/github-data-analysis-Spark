#!/bin/bash

# ======== Attention ========
# This script needs to be executed 
# after all nodes have executed init.sh
# which means all nodes have a password to login

configPath="/opt/module"
serverName=`hostname`

nodeList=(
    "master01"
    "master02"
    # "worker01"
    # "worker02"
    # "worker03"
    # "worker04"
)

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


for node in ${nodeList[@]}
do
    if [ $serverName != $node ]; then
        log_info "start to config $node"
        sshpass -p $initPassWd ssh-copy-id -i ~/.ssh/id_rsa.pub root@$node 
    fi
done