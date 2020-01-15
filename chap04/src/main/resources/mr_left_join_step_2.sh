#!/bin/bash
export JAVA_HOME=/usr/local/java/jdk1.8.0_231
echo "JAVA_HOME=$JAVA_HOME"
export BOOK_HOME=/opt/data_algorithms_book
export APP_JAR=/opt/data_algorithms_book/leftjoin/chap04-1.0-SNAPSHOT.jar
INPUT=/leftjoin/output/mr/setp_1/p*
OUTPUT=/leftjoin/output/mr/setp_2
hadoop fs -rm -r $OUTPUT
PROG=org.dataalgorithms.leftjoin.mapreduce.LocationCountDriver
hadoop jar $APP_JAR $PROG $INPUT $OUTPUT
