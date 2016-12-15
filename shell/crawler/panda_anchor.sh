#!/bin/bash

table_name=panda_anchor_crawler
jar=/home/likaiqing/hive-tool/live_crawler.jar
date=`date +%Y%m%d`
3_day_ago=`date -d '-3day $date' +%Y%m%d`
hour=`date +%H`
minute=`date +%M`
task=pandaanchor
/usr/local/jdk1.8.0_60/bin/java -jar $jar $task ${date} ${hour}

