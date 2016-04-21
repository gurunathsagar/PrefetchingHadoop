#!/bin/bash

#hadoop fs -mkdir -p /user/arjun/test2
#/home/hduser/hadoop/bin/hadoop fs -put /home/hduser/localinput/bigsample.txt /user/arjun/examples/input-data/map-reduce/
#/home/hduser/hadoop/bin/hadoop fs -copyFromLocal /home/hduser/big.txt /user/arjun/
#rm -rf /home/hduser/test/
#mkdir -p /home/hduser/test/$1
#mkdir -p /home/hduser/test/

#hdfs dfs -count -q /user/gurunath/wordcount/input-data


INP=$1
OUT=$2
DIR=`hdfs dfs -count -q $2 | sed 's/|/ /' | awk '{print $4}'`

javac SortDescending.java
java SortDescending $INP $OUT $DIR

status=$?

if [ $status = "0" ]; then
	echo "STATUS=SUCCESS"
	echo "COUNT=3"
else
	echo "STATUS=FAIL"
fi
