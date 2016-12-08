#!/bin/bash

date=$1
date=${date:=`date -d 'yesterday' +%Y%m%d`}

#sh /home/likaiqing/shell/crawler_detail_ana/crawler_day_detail_anchor_ana.sh $date
sh /home/likaiqing/shell/crawler_detail_ana/crawler_week_detail_anchor_ana.sh $date

hive -e "
set hive.cli.print.header=true;
select par_date as \`日期\` ,rid as \`主播号\`,name as \`主播昵称\`,plat as \`平台\`,category as \`分类\`,weight_str as \`体重\`,is_new as \`是否新主播(1:是,0:非)\`,live_time as \`直播时长(时)\`,rec_times as \`推荐次数\`,rec_times_r as \`推荐次数排名\`,max_pcu as PCU,cate_pcu_r as \`按分类PCU排名\`,plat_pcu_r as \`按平台PCU排名\`,pcu_raised as \`PCU增
长\`,pcu_raise_r as \`PCU增长排名\`,max_followers as \`订阅数\`,followers_raised as \`订阅数变化\` , followers_raise_r  as \`订阅数变化排名\` from panda_result.crawler_day_detail_anchor_ana where par_date='$date';" > /home/likaiqing/shell/crawler_ana/crawler_report/crawler_day_detail_anchor_ana_${date}.csv

hive -e "
set hive.cli.print.header=true;
select par_date as \`日期\` ,rid as \`主播号\`,name as \`主播昵称\`,plat as \`平台\`,category as \`分类\`,weight_str as \`体重\`,is_new as \`是否新主播(1:是,0:非)\`,live_time_per_day as \`平均直播时长(时每天)\`,rec_times as \`推荐次数\`,rec_times_r as \`推荐次数排名\`,max_pcu as PCU,cate_pcu_r as \`按分类PCU排名\`,plat_pcu_r as \`按平台PCU排名\`,pcu_raised as \`PCU增长\`,pcu_raise_r as \`PCU增长排名\`,max_followers as \`订阅数\`,followers_raised as \`订阅数变化\`,followers_raise_r as \`订阅数变化排名\` from panda_result.crawler_week_detail_anchor_ana where par_date='$date';" > /home/likaiqing/shell/crawler_ana/crawler_report/crawler_week_detail_anchor_ana_${date}.csv

zip /home/likaiqing/shell/crawler_ana/detail_crawler_ana_${date}.zip /home/likaiqing/shell/crawler_ana/crawler_report/*detail_anchor_ana_${date}.csv
