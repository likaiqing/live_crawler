#!/bin/bash

table_name=panda_realtime.panda_anchor_crawler
jar=/home/likaiqing/hive-tool/live_crawler.jar
date=`date +%Y%m%d`
hour=`date +%H`
task=jingpin
plat=douyu
for game in lol overwatch tvgame dota2
do
	/usr/local/jdk1.8.0_60/bin/java -jar $jar $task $plat $game $date $hour
	hive -e "alter table $table_name add if not exists partition(par_date='$date',par_hour='$hour') location '/bigdata/hive/panda_realtime/panda_anchor_crawler/$date$hour';"
	hive -e "load data local inpath '/data/tmp/crawler/${task}_${plat}_${date}_${hour}_${game}.csv' into table $table_name partition (par_date='$date',par_hour='$hour');"
done
