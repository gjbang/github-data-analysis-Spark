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



### Port

| Port | Service | Description |
| ---- | ------- | ----------- |
| 8088 | YARN | YARN ResourceManager |
| 9870 | HDFS | HDFS NameNode |
| 9868 | HDFS | HDFS SecondaryNameNode |
| 9864 | HDFS | HDFS DataNode |
| 8042 | YARN | YARN NodeManager |
| 8090 | Spark | Spark Master WebUI |
| 8091 | Spark | Spark Worker WebUI |
| 18080 | Spark | Spark History Server |


## Start Step

### Cluster Config and Start

All corresponding files and directories are located at `cluster/configs`

- modify `hosts` file with LF line ending, need to add one empty line at the end of the file
- copy `configs` directory to `$HOME` at master01
- ** modify priviledge of private key `ali-5003.pem` to `600` **
- add execution priviledge to all shell scripts in `configs/tools` directory
- run `sudo ./0-remoteConfig.sh` to config all nodes
- run `sudo ./1-remoteStart.sh` to start all nodes
