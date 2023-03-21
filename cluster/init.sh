#!/bin/bash

# Global and static variables

configPath="$HOME/env-config/"
sparkVersion="3.3.2"
hadoopVersion="3.3.2"
graphFrameVersion="graphframes-0.8.2-spark3.0-s_2.12.jar"

# Environment variables needed to add to bashrc
declare -A Environments
Environments=(
    ['PYSPARK_PYTHON']="export PYSPARK_PYTHON=python3"
    ['SPARK_HOME']="export SPARK_HOME=\"$HOME/env-config/spark\""
    ['SPARK_OPTS']="export SPARK_OPTS=\"--packages graphframes:graphframes:0.8.2-spark3.0-s_2.12\""
    ['SPARK_LOCAL_IP']="export SPARK_LOCAL_IP=\"127.0.0.1\""
    # ['$PYSPARK_DRIVER_PYTHON"']="export PYSPARK_DRIVER_PYTHON=\"jupyter\""
    # ['$PYSPARK_DRIVER_PYTHON_OPTS']="export PYSPARK_DRIVER_PYTHON_OPTS=\"notebook\""
    ['HADOOP_HOME']="export HADOOP_HOME=\"$configPath/hadoop\""
    ['HADOOP_CONF_DIR']="export HADOOP_CONF_DIR=\"$configPath/hadoop/etc/hadoop\""
    ['HADOOP_LOG_DIR']="export HADOOP_LOG_DIR=\"/var/log/hadoop\""
)


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


# ====== Basic System Config ======
system_config(){
    log_info "Start to update system configuration"
    log_info "apt update and install software"
    # java
    sudo add-apt-repository -y ppa:openjdk-r/ppa
    sudo apt-get -y update
    sudo apt-get -y install openjdk-8-jdk
    # install corresponding pip
    sudo apt -y install python3-pip

    log_info "pip install basic python package"
    pip3 install numpy matplotlib jupyterlab pyspark
}


# ====== Config Spark Basic Lib ======
spark_config(){

    # == create config file path
    log_info "create config path"
    if [ ! -d "$configPath" ]; then
        log_warn "$configPath not exists, create"
        cd $HOME
        mkdir env-config
    fi

    cd $configPath
    log_warn "current working path: `pwd`"

    log_info "install spark"

    # == download spark
    if [ ! -f "spark-$sparkVersion-bin-hadoop3.tgz" ]; then
        log_info "download spark, version: $sparkVersion"
        wget https://downloads.apache.org/spark/spark-$sparkVersion/spark-$sparkVersion-bin-hadoop3.tgz -P ./  -r -c -O "spark-$sparkVersion-bin-hadoop3.tgz"
        tar -zxvf spark-$sparkVersion-bin-hadoop3.tgz -C
        mv spark-$sparkVersion-bin-hadoop3 spark
    else
        log_warn "Spark $sparkVersion has existed!"
    fi

    # == download graphframe
    if [ ! -f "$graphFrameVersion" ]; then
        log_info "download GraphFrame"
        wget --content-disposition https://repos.spark-packages.org/graphframes/graphframes/0.8.2-spark3.0-s_2.12/graphframes-0.8.2-spark3.0-s_2.12.jar -P ./  -r -c -O "graphframes-0.8.2-spark3.0-s_2.12.jar"
        # install graphframe
        cp graphframes-0.8.2-spark3.0-s_2.12.jar $SPARK_HOME/jars/
    else
        log_warn "GraphFrame has existed!"
    fi

    # == set path environment vars
    log_info "set path environment vars"
    for key in ${!Environments[@]}; do
        if [ -z "${!key}" ]; then
            echo -e "${Environments[$key]}" >> ~/.bashrc
            log_info "${key} has been set"
        else
            log_warn "${key} has existed: ${!key}"
        fi
    done

    # == source bashrc
    source ~/.bashrc

    log_info "Spark config finished"
}


# ====== Config Hadoop Basic Lib ======
hadoop_config(){
    log_info "Start to config hadoop"
    # == download hadoop
    if [ ! -f "hadoop-$hadoopVersion.tar.gz" ]; then
        log_info "download hadoop, version: $hadoopVersion"
        wget https://archive.apache.org/dist/hadoop/common/hadoop-$hadoopVersion/hadoop-$hadoopVersion.tar.gz -P ./  -r -c -O "hadoop-$hadoopVersion.tar.gz"
        tar -zxvf hadoop-$hadoopVersion.tar.gz
        mv hadoop-$hadoopVersion hadoop
    else
        log_warn "Hadoop $hadoopVersion has existed!"
    fi

    # == move configure file to hadoop config path
    log_info "move configure file to hadoop config path"
    hadoopConfigDir="$configPath/hadoop/etc/hadoop/"
    mv $HOME/config/hadoop/* $hadoopConfigDir

    # == set privilege of hadoop
    log_info "set privilege of hadoop"
    chmod a+x $configPath/hadoop/sbin/start-dfs.sh
    chmod a+x $configPath/hadoop/sbin/start-yarn.sh

    # == create HDFS namenode and datanode directory
    log_info "create HDFS namenode and datanode directory"
    mkdir -p $configPath/hdfs/namenode
    mkdir -p $configPath/hdfs/datanode

    # == format namenode
    log_info "format namenode"
    $configPath/hadoop/bin/hdfs namenode -format

    log_info "Hadoop config finished"
}

system_config
spark_config
hadoop_config