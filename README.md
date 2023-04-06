# msbd5003-project

## Configuration

### Spark

Some basic configurations of Spark are as follows:

- `MASTER=spark://master01:7077` 
- `SPARK_MASTER_WEBUI_PORT=8090` : to avoid port conflict
- `SPARK_HISTORY_OPTS=-Dspark.history.fs.logDirectory=hdfs://master01:8020/user/hadoop/evtlogs`: the history log directory is set to HDFS: `/user/hadoop/evtlogs`

### Hive

There are two ways to configure Hive with Spark: `Spark on Hive` and `Hive on Spark`. The former is to use Hive as a metastore, and the latter is to use Hive as a SQL engine. In this project, we use the former way. Reasons are as followd:

- `Hive on Spark` need to recompile Spark and import `jar` packages, which is not convenient for us to use.
- Our main program will use pyspark, which is not compatible with Hive on Spark.
- More and more companies use SparkSQL to construct their data warehouse, and Hive is just a metastore.

#### Configs





## Start Step

### Cluster Config and Start

All corresponding files and directories are located at `./cluster`

- according to server info, set hosts using private IP address
  - not need to set `configs/hadoop/workers` and `configs/spark/slaves` 
  - get hostname from hosts file
- copy configs, test, and three shell scripts to $HOME
- `sudo chmod a+x *.sh`
- run `init.sh` firstly
- `source ~/.bashrc`
- after updating bash env and run `init.sh` on all nodes
    - run `nopasswdlog.sh` on each nodes
- run `start.sh` on master01 firstly
    - run `start.sh` on other nodes to start spark
    - or run `remote-start.sh` on master01 to start service on other nodes
