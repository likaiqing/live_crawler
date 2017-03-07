#!/bin/bash

java=$(which java)
table_name=panda_detail_anchor_crawler
log_path=/tmp
jar=/home/likaiqing/hive-tool/live_detail_anchor.jar
date=`date +%Y%m%d`
hour=`date +%H`
minute=`date +%M`
task=douyudetailanchor
$java -jar $jar $task ${date} ${hour}
