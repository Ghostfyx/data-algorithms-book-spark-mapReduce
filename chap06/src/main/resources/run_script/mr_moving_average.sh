#!/bin/bash
export JAVA_HOME=/usr/local/java/jdk1.8.0_231
echo "JAVA_HOME=$JAVA_HOME"
export BOOK_HOME=/opt/data_algorithms_book
export APP_JAR=/opt/data_algorithms_book/moving_average/chap06-1.0-SNAPSHOT.jar
export YARN_CONF=$HADOOP_HOME/etc/hadoop
export spark_home=/opt/spark/spark-2.4.4-bin-hadoop2.7
INPUT=/moving_average/input/timeSeries.txt
OUTPUT=/moving_average/output/mr
WINDOW=3
hadoop fs -rm -r $OUTPUT
PROG=org.dataalgorithms.movingaverage.mapreduce.SortByMRF_MovingAverageDriver
hadoop jar $APP_JAR $PROG $INPUT $OUTPUT $WINDOW
