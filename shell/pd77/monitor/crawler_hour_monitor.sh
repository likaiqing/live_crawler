#!/bin/bash

date=$1
date=${date:`date +%Y%m%d`}
hour=$2
hour=${hour:`date -d "-1 hour" +%k`}
if [ $hour -eq 23 ]; then
    date=`date -d "-1 day $date" +%Y%m%d`
fi

jar=/home/likaiqing/hive-tool/crawler_hour_monitor.jar
java=$(which java)
$java -jar $jar $date $hour