#!/bin/bash
# run MapReduce Secondary in RAM
export JAVA_HOME=/usr/local/java/jdk1.8.0_231
echo "JAVA_HOME=$JAVA_HOME"
export BOOK_HOME=/opt/data_algorithms_book
export APP_JAR=/opt/data_algorithms_book/topN/chap03-1.0-SNAPSHOT.jar
INPUT=/topN/input/cats.txt
OUTPUT=/topN/output/mr
hadoop fs -rmr $OUTPUT
PROG=org.dataalgorithms.topn.mapreduce.TopNDriver
hadoop jar $APP_JAR $PROG 10 $INPUT $OUTPUT

