#!/bin/bash

date=$1
hour=$2
date=${date:=`date +%Y%m%d`}
hour=${hour:=`date -d "-1 hour" +%H`}
if [ $hour = "23" ] && [ -z $1 ]; then
    date=`date -d "yesterday $date" +%Y%m%d`
fi
echo $date
echo $hour
remote_dir=/home/crawler_data/
current_dir=/data/tmp/likaiqing/crawler_data/
ssh root@222.186.169.41 "gzip ${remote_dir}${date}/${hour}/*.txt"
scp -p root@222.186.169.41:${remote_dir}${date}/${hour}/*anchor*.gz ${current_dir}crawler_anchor/
scp -p root@222.186.169.41:${remote_dir}${date}/${hour}/categorycrawler*.gz ${current_dir}crawler_category/
scp -p root@222.186.169.41:${remote_dir}${date}/${hour}/indexrec_*.gz ${current_dir}crawler_indexrec/

hadoop fs -put ${current_dir}crawler_anchor/*.gz /bigdata/hive/panda_competitor/crawler_anchor/$date/

hadoop fs -put /data/tmp/likaiqing/crawler_data/crawler_anchor/*.gz /bigdata/hive/panda_competitor/crawler_anchor/20170810/