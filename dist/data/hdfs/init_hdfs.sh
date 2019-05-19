#!/bin/bash

hdfs namenode -format
$HADOOP_HOME/sbin/start-dfs.sh
