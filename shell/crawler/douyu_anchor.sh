#!/bin/bash

table_name=panda_anchor_crawler
jar=/home/likaiqing/hive-tool/live_crawler.jar
date=`date +%Y%m%d`
3_day_ago=`date -d '-3day $date' +%Y%m%d`
hour=`date +%H`
minute=`date +%M`
task=douyuanchor
/usr/local/jdk1.8.0_60/bin/java -jar $jar $task ${date} ${hour}
hive -e "alter table panda_realtime.$table_name add if not exists partition(par_date='$date',hour='$hour') location '/bigdata/hive/panda_realtime/$table_name/$date$hour';"
hive -e "load data local inpath '/data/tmp/crawler/${task}_${date}_${hour}.csv' into table panda_realtime.$table_name partition (par_date='$date',hour='$hour');"
mv /data/tmp/crawler/${task}_${date}_${hour}.csv /data/tmp/crawler/${task}_${date}_${hour}_${minute}.csv
mv /data/tmp/crawler/log/${task}.log /data/tmp/crawler/log/${task}_${date}_${hour}_${minute}.log
rm /data/tmp/crawler/${task}_${3_day_ago}_${hour}_${minute}.csv /data/tmp/crawler/log/${task}_${3_day_ago}_${hour}_${minute}.log 
