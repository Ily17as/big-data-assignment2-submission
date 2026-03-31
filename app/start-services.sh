#!/bin/bash
set -euo pipefail

$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh
mapred --daemon start historyserver

# Quick cluster sanity check after startup.
jps -lm
hdfs dfsadmin -report || true
hdfs dfsadmin -safemode leave || true

hdfs dfs -mkdir -p /apps/spark/jars
hdfs dfs -chmod 744 /apps/spark/jars || true
hdfs dfs -put -f /usr/local/spark/jars/* /apps/spark/jars/ || true
hdfs dfs -chmod +rx /apps/spark/jars/* || true
hdfs dfs -mkdir -p /user/root || true