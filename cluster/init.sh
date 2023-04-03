#!/bin/bash

# Global and static variables

configPath="/opt/module"
sparkVersion="3.3.2"
hadoopVersion="3.3.2"
graphFrameVersion="graphframes-0.8.2-spark3.0-s_2.12.jar"
zookeeperVersion="3.7.1"
scalaVersion="2.13"
kafkaVersion="3.4.0"
flumeVersion="1.11.0"
hiveVersion="3.1.2"
serverName=`hostname`


# param list to execute different config function
declare -A paramDict
paramDict=(
    ['system']="system_config"
    ['spark']="spark_config"
    ['hadoop']="hadoop_config"
    ['zookeeper']="zookeeper_config"
    ['scala']="scala_config"
    ['kafka']="kafka_config"
    ['flume']="flume_config"
    ['hive']="hive_config"
    ['ssh']="ssh_config"
)
paramList=(
    "system"
    "spark"
    "hadoop"
    "zookeeper"
    "scala"
    "kafka"
    "flume"
    "hive"
    "ssh"
    )


# Environment variables needed to add to bashrc
declare -A Environments
Environments=(
    ['PYSPARK_PYTHON']="export PYSPARK_PYTHON=python3"
    ['SPARK_HOME']="export SPARK_HOME=\"$configPath/spark\""
    ['SPARK_OPTS']="export SPARK_OPTS=\"--packages graphframes:graphframes:0.8.2-spark3.0-s_2.12\""
    ['SPARK_LOCAL_IP']="export SPARK_LOCAL_IP=\"127.0.0.1\""
    # ['$PYSPARK_DRIVER_PYTHON"']="export PYSPARK_DRIVER_PYTHON=\"jupyter\""
    # ['$PYSPARK_DRIVER_PYTHON_OPTS']="export PYSPARK_DRIVER_PYTHON_OPTS=\"notebook\""
    ['HADOOP_HOME']="export HADOOP_HOME=\"$configPath/hadoop\""
    ['HADOOP_CONF_DIR']="export HADOOP_CONF_DIR=\"$configPath/hadoop/etc/hadoop\""
    ['HADOOP_LOG_DIR']="export HADOOP_LOG_DIR=\"/var/log/hadoop\""
    ['KAFKA_HOME']="export KAFKA_HOME=\"$configPath/kafka\""
    ['HIVE_HOME']="export HIVE_HOME=\"$configPath/hive\""
    ['JAVA_HOME']="export HIVE_HOME=\"/usr/lib/jvm/java-8-openjdk-amd64\""
)

# generate different kafka block id config file for each node
declare -A kafkaBlockId
kafkaBlockId=(
    ['master01']="broker.id=1"
    ['master02']="broker.id=2"
    ['worker01']="broker.id=3"
    ['worker02']="broker.id=4"
    ['worker03']="broker.id=5"
    ['worker04']="broker.id=6"
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
    sudo update-java-alternatives --set java-1.8.0-openjdk-amd64
    # install corresponding pip
    sudo apt -y install python3-pip

    log_info "pip install basic python package"
    pip3 install numpy matplotlib jupyterlab pyspark

    log_info "add host mapping"
    cat $HOME/configs/hosts >> /etc/hosts

    # == create config file path
    log_info "create config path"
    if [ ! -d "$configPath" ]; then
        log_warn "$configPath not exists, create"
        cd /opt
        mkdir module
    fi
}


# ====== Config Spark Basic Lib ======
spark_config(){



    cd $configPath
    log_warn "current working path: `pwd`"

    log_info "install spark"

    # == download spark
    if [ ! -f "spark-$sparkVersion-bin-hadoop3.tgz" ]; then
        log_info "download spark, version: $sparkVersion"
        wget https://mirrors.tuna.tsinghua.edu.cn/apache/spark/spark-$sparkVersion/spark-$sparkVersion-bin-hadoop3.tgz -P ./  -r -c -O "spark-$sparkVersion-bin-hadoop3.tgz"
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
        cp graphframes-0.8.2-spark3.0-s_2.12.jar $configPath/spark/jars/
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

    cd $configPath
    log_warn "current working path: `pwd`"

    log_info "Start to config hadoop"
    # == download hadoop
    if [ ! -f "hadoop-$hadoopVersion.tar.gz" ]; then
        log_info "download hadoop, version: $hadoopVersion"
        # wget https://archive.apache.org/dist/hadoop/common/hadoop-$hadoopVersion/hadoop-$hadoopVersion.tar.gz -P ./  -r -c -O "hadoop-$hadoopVersion.tar.gz"
        wget https://mirrors.tuna.tsinghua.edu.cn/apache/hadoop/common/hadoop-$hadoopVersion/hadoop-$hadoopVersion.tar.gz -P ./  -r -c -O "hadoop-$hadoopVersion.tar.gz"
    else
        log_warn "Hadoop $hadoopVersion has existed!"
    fi

    # == untar hadoop
    tar -zxvf hadoop-$hadoopVersion.tar.gz
    mv hadoop-$hadoopVersion hadoop

    # == move configure file to hadoop config path
    log_info "move configure file to hadoop config path"
    hadoopConfigDir="$configPath/hadoop/etc/hadoop/"
    mv $HOME/configs/hadoop/* $hadoopConfigDir

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



# ====== zookeeper config ======
zookeeper_config(){
    
    cd $configPath    
    log_warn "current working path: `pwd`"

    # ====== delete old zookeeper ======
    if [ -d "zookeeper" ]; then
        log_info "delete old zookeeper"
        rm -rf zookeeper
    fi

    if [ ! -f "zookeeper-$zookeeperVersion-bin.tar.gz" ]; then
        # download
        log_info "download zookeeper, version: $zookeeperVersion"
        wget https://mirrors.tuna.tsinghua.edu.cn/apache/zookeeper/zookeeper-$zookeeperVersion/apache-zookeeper-$zookeeperVersion-bin.tar.gz -P ./  -r -c -O "zookeeper-$zookeeperVersion-bin.tar.gz"
    else
        log_warn "Zookeeper $zookeeperVersion has existed!"
    fi

    # decompress
    tar -zxvf zookeeper-$zookeeperVersion-bin.tar.gz
    # rename
    mv zookeeper-$zookeeperVersion-bin zookeeper

    # ======= configs =======
    mkdir /opt/module/zookeeper/zkData
    mv $HOME/configs/zookeeper/zoo.cfg $configPath/zookeeper/conf/zoo.cfg

}


# ====== kafka config ======
kafka_config(){
    cd $configPath
    log_warn "current working path: `pwd`"

    # ===== delete old kafka =====
    if [ -d "$configPath/kafka" ]; then
        log_warn "delete old kafka"
        rm -rf $configPath/kafka
    fi

    if [ ! -f "kafka_$scalaVersion-$kafkaVersion.tgz" ]; then
        # download
        log_info "download kafka, version: $kafkaVersion"
        wget https://mirrors.tuna.tsinghua.edu.cn/apache/kafka/$kafkaVersion/kafka_$scalaVersion-$kafkaVersion.tgz -P ./  -r -c -O "kafka_$scalaVersion-$kafkaVersion.tgz"
    else
        log_warn "Kafka $kafkaVersion has existed!"
    fi

    # decompress
    tar -zxvf kafka_$scalaVersion-$kafkaVersion.tgz
    # rename
    mv kafka_$scalaVersion-$kafkaVersion kafka

    #config
    mkdir $configPath/kafka/datas
    mv $HOME/configs/kafka/server.properties $configPath/kafka/config/server.properties
    # ! change broker.id ---- vim /opt/module/kafka/config/server.properties
    find -name "$configPath/kafka/config/server.properties" | xargs perl -pi -e "s|broker.id=0|${kafkaBlockId[$serverName]}|g"
}


# ====== flume config ======
flume_config(){
    cd $configPath
    log_warn "current working path: `pwd`"

    # ===== delete old flume =====
    if [ -d "flume" ]; then
        log_info "delete old flume"
        rm -rf flume
    fi

    if [ ! -f "apache-flume-$flumeVersion-bin.tar.gz" ]; then
        # download
        log_info "download flume, version: $flumeVersion"
        wget https://mirrors.tuna.tsinghua.edu.cn/apache/flume/$flumeVersion/apache-flume-$flumeVersion-bin.tar.gz -P ./  -r -c -O "apache-flume-$flumeVersion-bin.tar.gz"
    else
        log_warn "Flume $flumeVersion has existed!"
    fi

    # decompress
    tar -zxvf apache-flume-$flumeVersion-bin.tar.gz
    # rename
    mv apache-flume-$flumeVersion-bin flume

    # remove useless jar lib
    rm -f $configPath/flume/lib/guava-11.0.2.jar
    # rm -f $configPath/flume/lib/guava-*.jar
}


hive_config(){
    cd $configPath
    log_warn "current working path: `pwd`"

    # ===== delete old hive =====
    if [ -d "hive" ]; then
        log_info "delete old hive"
        rm -rf hive
    fi

    # ===== download hive =====
    if [ ! -f "apache-hive-$hiveVersion-bin.tar.gz" ]; then
        # download
        log_info "download hive, version: $hiveVersion"
        wget https://mirrors.tuna.tsinghua.edu.cn/apache/hive/hive-$hiveVersion/apache-hive-$hiveVersion-bin.tar.gz -P ./  -r -c -O "apache-hive-$hiveVersion-bin.tar.gz"
    else
        log_warn "Hive $hiveVersion has existed!"
    fi

    # decompress
    tar -zxvf apache-hive-$hiveVersion-bin.tar.gz
    # rename
    mv apache-hive-$hiveVersion-bin hive
    # delete old guava-19.0.jar
    rm -f $configPath/hive/lib/guava-19.0.jar 
    # copy guava-19.0.jar from hadoop share
    cp $configPath/hadoop/share/hadoop/common/lib/guava-27.0-jre.jar $configPath/hive/lib/
    # init hive metadata
    $configPath/hive/bin/schematool -dbType derby -initSchema
}


ssh_config(){

    log_info "ssh config"
    ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
    chmod 600 ~/.ssh/authorized_keys

    echo -e "Port 22\nPubkeyAuthentication yes\n" >> /etc/ssh/sshd_config 
    find -name "/etc/ssh/sshd_config " | xargs perl -pi -e "s|PermitRootLogin no|PermitRootLogin yes|g"
}


# ====== main execution process ======
# check if execute all configs
paramNum=$#
if [ $paramNum -eq 0 ]; then
    log_info "No args, use default config, execute all config"
fi

# == execute config for different args
for param in ${paramList[@]}
do
    if echo $@ | grep -q "\b$param\b"; then
        log_info "execute config for $param"
        ${paramDict[$param]}
    elif [ $paramNum -eq 0 ]; then
        log_info "execute config for $param"
        ${paramDict[$param]}
    else
        log_warn "No config for $param"
    fi
done