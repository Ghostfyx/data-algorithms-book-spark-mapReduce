#!/bin/bash
# run MapReduce Secondary in RAM
export JAVA_HOME=/usr/local/java/jdk1.8.0_231
echo "JAVA_HOME=$JAVA_HOME"
export BOOK_HOME=/opt/data_algorithms_book
export APP_JAR=/opt/data_algorithms_book/secondary_sort/chap01-1.0-SNAPSHOT.jar
INPUT=/secondary_sort/input/weather_input.txt
OUTPUT=/secondary_sort/output/mr_ram
hadoop fs -rmr $OUTPUT
PROG=mapreduce.SecondarySortDriver
hadoop jar $APP_JAR $PROG $INPUT $OUTPUT
