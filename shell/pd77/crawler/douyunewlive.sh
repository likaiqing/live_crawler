#!/bin/bash

jar=/home/likaiqing/hive-tool/live_detail_anchor.jar
date=`date +%Y%m%d`
hour=`date +%H`
task=douyunewlive
/usr/bin/java -jar $jar $task ${date} ${hour}
