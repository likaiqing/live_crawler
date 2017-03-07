#!/bin/bash

java=$(which java)
jar=/home/likaiqing/hive-tool/live_detail_anchor.jar
date=`date +%Y%m%d`
hour=`date +%H`
task=pandadetailanchor
$java -jar $jar $task ${date} ${hour}
