#!/bin/bash

table_name=panda_detail_anchor_crawler
log_path=/data/tmp/crawler_log
jar=/home/likaiqing/hive-tool/live_detail_anchor.jar
date=`date +%Y%m%d`
3_day_ago=`date -d '-3day $date' +%Y%m%d`
hour=`date +%H`
minute=`date +%M`
task=huyadetailanchor
/usr/bin/java -jar $jar $task ${date} ${hour}
