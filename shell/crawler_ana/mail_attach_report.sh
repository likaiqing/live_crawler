#!/bin/bash

date=$1
date=${date:=`date -d 'yesterday' +%Y%m%d`}
jar=/home/likaiqing/hive-tool/mail_attach.jar

/usr/local/jdk1.8.0_60/bin/java -jar $jar "爬取主播分析:$date" "报表见附件" "/home/likaiqing/shell/crawler_ana/crawler_ana_${date}.zip"
