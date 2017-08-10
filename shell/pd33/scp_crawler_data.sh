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

for d in crawler_anchor crawler_category crawler_indexrec crawler_detail_anchor crawler_gift_id crawler_twitch_category crawler_twitch_channel crawler_twitch_detail_channel
do
    if [ ! -d ${current_dir}$d ]; then
        mkdir -p ${current_dir}$d
    fi
done

#抓取列表,分类,推荐
ssh root@222.186.169.41 "gzip ${remote_dir}${date}/${hour}/*.txt"
scp -p root@222.186.169.41:${remote_dir}${date}/${hour}/*anchor*.gz ${current_dir}crawler_anchor/
scp -p root@222.186.169.41:${remote_dir}${date}/${hour}/categorycrawler*.gz ${current_dir}crawler_category/
scp -p root@222.186.169.41:${remote_dir}${date}/${hour}/indexrec_*.gz ${current_dir}crawler_indexrec/

hadoop fs -put ${current_dir}crawler_anchor/*.gz /bigdata/hive/panda_competitor/crawler_anchor/$date/
hadoop fs -put ${current_dir}crawler_category/*.gz /bigdata/hive/panda_competitor/crawler_category/$date/
hadoop fs -put ${current_dir}crawler_indexrec/*.gz /bigdata/hive/panda_competitor/crawler_indexrec_detail_anchor/$date/

rm -rf ${current_dir}crawler_anchor/*.gz
rm -rf ${current_dir}crawler_category/*.gz
rm -rf ${current_dir}crawler_indexrec/*.gz

#抓取详情,斗鱼礼物id
ssh root@180.97.220.220 "gzip ${remote_dir}${date}/${hour}/*.txt"
scp -p root@180.97.220.220:${remote_dir}${date}/${hour}/*detailanchor*.gz ${current_dir}crawler_detail_anchor/
scp -p root@180.97.220.220:${remote_dir}${date}/${hour}/douyugiftid*.gz ${current_dir}crawler_gift_id/


hadoop fs -put ${current_dir}crawler_detail_anchor/*.gz /bigdata/hive/panda_competitor/crawler_detail_anchor/$date/
hadoop fs -put ${current_dir}crawler_gift_id/*.gz /bigdata/hive/panda_competitor/crawler_gift_id/$date/

rm -rf ${current_dir}crawler_detail_anchor/*.gz
rm -rf ${current_dir}crawler_gift_id/*.gz


#twitch
ssh root@180.97.220.166 "gzip ${remote_dir}${date}/${hour}/*.txt"
scp -p root@180.97.220.166:${remote_dir}${date}/${hour}/twitchcategory*.gz ${current_dir}crawler_twitch_category/
scp -p root@180.97.220.166:${remote_dir}${date}/${hour}/twitchlist*.gz ${current_dir}crawler_twitch_channel/
scp -p root@180.97.220.166:${remote_dir}${date}/${hour}/twitchdetailchannel_*.gz ${current_dir}crawler_twitch_detail_channel/


hadoop fs -put ${current_dir}crawler_twitch_category/*.gz /bigdata/hive/panda_competitor/crawler_twitch_category/$date/
hadoop fs -put ${current_dir}crawler_twitch_channel/*.gz /bigdata/hive/panda_competitor/crawler_twitch_channel/$date/
hadoop fs -put ${current_dir}crawler_twitch_detail_channel/*.gz /bigdata/hive/panda_competitor/crawler_twitch_detail_channel/$date/

rm -rf ${current_dir}crawler_twitch_category/*.gz
rm -rf ${current_dir}crawler_twitch_channel/*.gz
rm -rf ${current_dir}crawler_twitch_detail_channel/*.gz