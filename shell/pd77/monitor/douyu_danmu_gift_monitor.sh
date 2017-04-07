#!/bin/bash

date=$1
date=${date:=`date -d "-1 day" +%Y%m%d`}
jar=/home/likaiqing/hive-tool/douyu_danmu_gift_monitor.jar
java=$(which java)

$java -jar $jar $date