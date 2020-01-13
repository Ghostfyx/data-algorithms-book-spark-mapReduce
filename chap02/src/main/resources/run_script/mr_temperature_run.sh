#!/bin/bash
# run MapReduce Secondary in RAM
export JAVA_HOME=/usr/local/java/jdk1.8.0_231
echo "JAVA_HOME=$JAVA_HOME"
export BOOK_HOME=/opt/data_algorithms_book
export APP_JAR=/opt/data_algorithms_book/secondary_sort/chap02-1.0-SNAPSHOT.jar
INPUT=/secondary_sort/input/stock_symbol.txt
OUTPUT=/secondary_sort/output/mr
hadoop fs -rmr $OUTPUT
PROG=org.dataalgorithms.chap02.mapreduce.SecondarySortDriver
hadoop jar $APP_JAR $PROG $INPUT $OUTPUT

