#!/bin/bash

date=$1
date=${date:=`date -d 'yesterday' +%Y%m%d`}

/usr/local/jdk1.8.0_60/bin/java -jar /home/likaiqing/hive-tool/anchor_crawler_mail.jar $date
