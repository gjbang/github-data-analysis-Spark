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
mysqlVersion="0.8.24-1"
jdbcVersion="8.0.32-1"
hbaseVersion="2.5.3"
serverName=`hostname`
initPassWd="heikediguo"

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
    ['mysql']="mysql_config"
    ['hbase']="hbase_config"
)
paramList=(
    "system"
    "spark"
    "hadoop"
    "zookeeper"
    "scala"
    "kafka"
    "flume"
    "mysql"
    "hbase"
    "hive"
    "ssh"
    )
# record all nodes name for initialize slaves, workers, etc.
nodeList=()

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
    ['JAVA_HOME']="export JAVA_HOME=\"/usr/lib/jvm/java-8-openjdk-amd64\""
    ['HBASE_HOME']="export HBASE_HOME=\"$configPath/hbase\""
    ['PATH']="export PATH=\$PATH:\$SPARK_HOME/bin:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$KAFKA_HOME/bin:\$HIVE_HOME/bin:\$HBASE_HOME/bin"
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
    sudo apt-get -y install openjdk-8-jdk sshpass
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
    else
        log_warn "Spark $sparkVersion has existed!"
    fi

    tar -zxvf spark-$sparkVersion-bin-hadoop3.tgz
    mv spark-$sparkVersion-bin-hadoop3 spark

    # == download graphframe
    if [ ! -f "$graphFrameVersion" ]; then
        log_info "download GraphFrame"
        wget --content-disposition https://repos.spark-packages.org/graphframes/graphframes/0.8.2-spark3.0-s_2.12/graphframes-0.8.2-spark3.0-s_2.12.jar -P ./  -r -c -O "graphframes-0.8.2-spark3.0-s_2.12.jar"
        # install graphframe
        cp graphframes-0.8.2-spark3.0-s_2.12.jar $configPath/spark/jars/
    else
        log_warn "GraphFrame has existed!"
    fi

    # == config spark env vars == #
    mv $HOME/configs/spark/* $configPath/spark/conf/
    # if not config local ip, web ui may mapping to localhost:xxxx
    # which cannot be visited by outer network
    # not need to modify /etc/hosts
    # corresponding: netstat -ap | grep java; ps -ef | grep spark
    echo -e "export SPARK_LOCAL_IP=$serverName\n" >> $configPath/spark/conf/spark-env.sh
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
    mv apache-zookeeper-$zookeeperVersion-bin zookeeper

    # ======= configs =======
    mkdir /opt/module/zookeeper/zkData
    mv $HOME/configs/zookeeper/zoo.cfg $configPath/zookeeper/conf/zoo.cfg
    mv $HOME/configs/zookeeper/myid $configPath/zookeeper/zkData/myid

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
    find $configPath/kafka/config/ -name "server.properties" | xargs perl -pi -e "s|broker.id=0|${kafkaBlockId[$serverName]}|g"
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
    mv $HOME/configs/flume/flume-env.sh $configPath/flume/conf/flume-env.sh
}




mysql_config(){
    cd $configPath
    log_warn "current working path: `pwd`"

    # ===== delete old mysql =====
    if type "mysql" > /dev/null; then
        log_info "delete old mysql"
        sudo DEBIAN_FRONTEND=noninteractive apt-get autoremove --purge "mysql*" -y 
        sudo DEBIAN_FRONTEND=noninteractive rm -rf /etc/mysql/ /var/lib/mysql
    fi

    # ===== download mysql =====
    if ! type "mysql" > /dev/null; then
        # download
        log_info "download mysql, version: $mysqlVersion"
        wget "https://dev.mysql.com/get/mysql-apt-config_${mysqlVersion}_all.deb" -P ./  -r -c -O "mysql-apt-config_${mysqlVersion}_all.deb"
        sudo DEBIAN_FRONTEND=noninteractive dpkg -i mysql-apt-config_${mysqlVersion}_all.deb

        # install mysql from apt repo
        sudo apt update
        sudo DEBIAN_FRONTEND=noninteractive apt-get -yq install mysql-server
        sudo DEBIAN_FRONTEND=noninteractive apt-get -yq install mysql-client

        # ====== configs user passwd and privileges =======
        mysql -uroot < $HOME/configs/mysql/config.sql

        # ====== start mysql =======
        sudo service mysql restart

        # open port
        sudo ufw allow 3306
    else
        log_warn "Mysql $mysqlVersion has existed!"
    fi

}



hbase_config(){
    cd $configPath
    log_warn "current working path: `pwd`"

    # ===== delete old hbase =====
    if [ -d "hbase" ]; then
        log_info "delete old hbase"
        rm -rf hbase
    fi

    # ===== download hbase =====
    if [ ! -f "hbase-$hbaseVersion-bin.tar.gz" ]; then
        # download
        log_info "download hbase, version: $hbaseVersion"
        wget https://mirrors.tuna.tsinghua.edu.cn/apache/hbase/$hbaseVersion/hbase-$hbaseVersion-bin.tar.gz -P ./  -r -c -O "hbase-$hbaseVersion-bin.tar.gz"
    else
        log_warn "Hbase $hbaseVersion has existed!"
    fi

    # decompress
    tar -zxvf hbase-$hbaseVersion-bin.tar.gz
    # rename
    mv hbase-$hbaseVersion hbase

    mv $HOME/configs/hbase/* $configPath/hbase/conf/
}


# ====== hive config ======
# 1. download hive and decompress
# 2. adjust outer lib (JDBC, etc)
# 3. init with MySQL
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
    # rename and logs path
    mv apache-hive-$hiveVersion-bin hive
    mkdir $configPath/hive/logs/

    # === config outer lib for hive ===

    # delete old guava-19.0.jar
    rm -f $configPath/hive/lib/guava-19.0.jar 
    # copy guava-19.0.jar from hadoop share
    cp $configPath/hadoop/share/hadoop/common/lib/guava-27.0-jre.jar $configPath/hive/lib/

    # config MySQL JDBC
    wget "https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-j_8.0.32-1ubuntu22.04_all.deb" -P ./  -r -c -O "mysql-jdbc.deb"
    DEBIAN_FRONTEND=noninteractive dpkg -i ./mysql-jdbc.deb
    cp /usr/share/java/mysql-connector-j-8.0.32.jar $configPath/hive/lib/

    # === move config files ===
    mv $HOME/configs/hive/* $configPath/hive/conf/

    # init hive metadata
    $configPath/hive/bin/schematool -dbType mysql -initSchema -verbose
}


# help to config ssh, main function:
# 1. generate ssh key
# 2. config local ssh authorized_keys
# 3. config sshd_config: all key/password login, 
# 4. create root password
ssh_config(){
    # generate ssh key
    log_info "ssh config"
    ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
    chmod 600 ~/.ssh/authorized_keys

    # config ssh
    echo -e "Port 22\nPubkeyAuthentication yes\n" >> /etc/ssh/sshd_config 
    find /etc/ssh/ -name sshd_config | xargs perl -pi -e "s|PermitRootLogin no|PermitRootLogin yes|g"
    # open password login, for ssh-copy id
    find /etc/ssh/ -name sshd_config | xargs perl -pi -e "s|PasswordAuthentication no|PasswordAuthentication yes|g"
    # not input yes when ssh-copy-id
    sed -i '/StrictHostKeyChecking/c StrictHostKeyChecking no' /etc/ssh/ssh_config

    # set a password for root, for ssh-copy-id
    echo -e "root:$initPassWd" | chpasswd

    # update config
    service ssh restart
}


# directly get the node list from hosts, not need to config manually
generate_node_list(){
    # generate worker list
    log_info "generate worker list"

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

    # write node list to slaves, workers, etc
    cat /dev/null > $HOME/configs/spark/slaves
    cat /dev/null > $HOME/configs/hadoop/workers
    cat /dev/null > $HOME/configs/hbase/regionservers
    cat /dev/null > $HOME/configs/hbase/backup-masters
    for node in ${nodeList[@]}; do
        echo $node >> $HOME/configs/spark/slaves
        echo $node >> $HOME/configs/hadoop/workers
        echo $node >> $HOME/configs/hbase/regionservers
        if [ $node != "master01" ]; then
            echo $node >> $HOME/configs/hbase/backup-masters
        fi
    done


}

# ====== main execution process ======
# check if execute all configs
paramNum=$#
if [ $paramNum -eq 0 ]; then
    log_info "No args, use default config, execute all config"
fi

# == generate node name from hosts file ==
generate_node_list

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