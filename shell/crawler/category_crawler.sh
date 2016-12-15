#!/bin/bash

jar=/home/likaiqing/hive-tool/live_crawler.jar
date=`date +%Y%m%d`
hour=`date +%H`
task=categorycrawler
/usr/local/jdk1.8.0_60/bin/java -jar $jar ${task} ${date} ${hour}