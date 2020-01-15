#!/bin/bash
export JAVA_HOME=/usr/local/java/jdk1.8.0_231
echo "JAVA_HOME=$JAVA_HOME"
export BOOK_HOME=/opt/data_algorithms_book
export APP_JAR=/opt/data_algorithms_book/leftjoin/chap04-1.0-SNAPSHOT.jar
export HADOOP_HOME=/opt/hadoop/hadoop-2.7.7
export HADOOP_CONF=$HADOOP_HOME/etc/hadoop
export YARN_CONF=$HADOOP_HOME/etc/hadoop
export spark_home=/opt/spark/spark-2.4.4-bin-hadoop2.7
USERS=/leftjoin/input/users.txt
TRANSACTIONS=/leftjoin/input/transactions.txt
OUTPUT=/leftjoin/output/spark
prog=org.dataalgorithms.leftjoin.spark.SparkUseLeftJoin
$SPARK_HOME/bin/spark-submit \
 --class $prog \
 --master yarn \
 --deploy-mode client \
 --executor-memory 2G\
 --num-executors 10 \
 $APP_JAR \
 $USERS \
 $TRANSACTIONS \
 $OUTPUT
