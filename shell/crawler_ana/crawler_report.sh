#!/bin/bash

date=$1
date=${date:=`date -d 'yesterday' +%Y%m%d`}

#sh /home/likaiqing/shell/crawler_ana/crawler_distinct_anchor.sh $date
#sh /home/likaiqing/shell/crawler_ana/crawler_distinct_detail_anchor.sh $date

sh /home/likaiqing/shell/crawler_ana/crawler_day_anchor_ana.sh $date
sh /home/likaiqing/shell/crawler_ana/crawler_week_anchor_ana.sh $date
sh /home/likaiqing/shell/crawler_ana/crawler_day_broadcast_ana.sh $date
sh /home/likaiqing/shell/crawler_ana/crawler_week_broadcast_ana.sh $date
sh /home/likaiqing/shell/crawler_ana/crawler_day_plat_ana.sh $date
sh /home/likaiqing/shell/crawler_ana/crawler_week_plat.ana.sh $date

sh /home/likaiqing/shell/crawler_detail_ana/crawler_day_detail_anchor_ana.sh $date
sh /home/likaiqing/shell/crawler_detail_ana/crawler_week_detail_anchor_ana.sh $date

ssh 10.110.20.77 "sh /home/likaiqing/shell/crawler_ana/export2excel.sh $date"

#export2exceljar=/home/likaiqing/hive-tool/export2excel.jar

#/usr/local/jdk1.8.0_60/bin/java -jar $export2exceljar $date panda_result.crawler_day_anchor_ana par_date,rid,name,plat,category,rec_times,max_pcu,pcu_raised,fol_changed,live_time,is_new,cate_pcu_rank,plat_pcu_rank,fol_rank,rec_times_rank "日期,主播号,主播昵称,平台,分类,推荐次数,PCU,PCU增长,订阅数变化,直播时长(时),是否新主播(1:是;0:非),分类PCU排名,平台PCU排名,订阅排名,推荐次数排名" /home/likaiqing/shell/crawler_ana/crawler_report/

#hive -e "
#set hive.cli.print.header=true;
#select par_date as \`日期\` ,rid as \`主播号\`,name as \`主播昵称\`,plat as \`平台\`,category as \`分类\`,rec_times as \`推荐次数\`,max_pcu as PCU,pcu_raised as \`PCU增长\`,fol_changed as \`订阅数变化\`,live_time as \`直播时长(时)\`,is_new as \`是否新主播(1:是,0:非)\`,cate_pcu_rank as \`分类PCU排名\`,plat_pcu_rank as \`平台PCU排名\`,fol_rank as \`订阅排名\`,rec_times_rank as \`推荐次数排名\` from panda_result.crawler_day_anchor_ana where par_date='$date';
#" > /home/likaiqing/shell/crawler_ana/crawler_report/crawler_day_anchor_ana_${date}.csv
#iconv -f UTF-8 -c  -t GBK /home/likaiqing/shell/crawler_ana/crawler_report/crawler_day_anchor_ana_${date}.csv > /home/likaiqing/shell/crawler_ana/crawler_report/crawler_day_anchor_ana_${date}_gbk.csv
#rm -rf /home/likaiqing/shell/crawler_ana/crawler_report/crawler_day_anchor_ana_${date}.csv


#/usr/local/jdk1.8.0_60/bin/java -jar $export2exceljar $date panda_result.crawler_week_anchor_ana par_date,rid,name,plat,category,rec_times,max_pcu,pcu_raised,fol_changed,live_time_avg,is_new,pcu_rank,fol_rank,rec_times_rank "日期,主播号,主播昵称,平台,分类,推荐次数,PCU,PCU增长,订阅数变化,平均直播时长(时每天),是否新主播(1:是;0:非),分类PCU排名,订阅排名,推荐次数排名" /home/likaiqing/shell/crawler_ana/crawler_report/

#hive -e "
#set hive.cli.print.header=true;
#select par_date as \`日期\` ,rid as \`主播号\`,name as \`主播昵称\`,plat as \`平台\`,category as \`分类\`,rec_times as \`推荐次数\`,max_pcu as PCU,pcu_raised as \`PCU增长\`,fol_changed as \`订阅数变化\`,live_time_avg as \`平均直播时长(时每天)\`,is_new as \`是否新主播(1:是 0:非)\`,pcu_rank as \`PCU排名\`,fol_rank as \`订阅排名\`,rec_times_rank as \`推荐次数排名\` from panda_result.crawler_week_anchor_ana where par_date='$date';
#" > /home/likaiqing/shell/crawler_ana/crawler_report/crawler_week_anchor_ana_${date}.csv
#iconv -f UTF-8 -c  -t GBK /home/likaiqing/shell/crawler_ana/crawler_report/crawler_week_anchor_ana_${date}.csv > /home/likaiqing/shell/crawler_ana/crawler_report/crawler_week_anchor_ana_${date}_gbk.csv
#rm -rf /home/likaiqing/shell/crawler_ana/crawler_report/crawler_week_anchor_ana_${date}.csv

#/usr/local/jdk1.8.0_60/bin/java -jar $export2exceljar $date panda_result.crawler_day_broadcast_ana par_date,plat,lives,live_time_avg,pcu_avg,new_flag "日期,平台,开播数,平均开播时间(时每天),平均PCU,是否新主播(1=Y;0=N;2=ALL)" /home/likaiqing/shell/crawler_ana/crawler_report/

#hive -e "
#set hive.cli.print.header=true;
#select par_date as \`日期\`,plat as \`平台\` ,lives as \`开播数\`,live_time_avg as \`平均开播时间(时每天)\`,pcu_avg as \`平均PCU()\`,new_flag as \`是否新主播(1=Y 0=N 2=ALL)\` from panda_result.crawler_day_broadcast_ana where par_date='$date';
#" > /home/likaiqing/shell/crawler_ana/crawler_report/crawler_day_broadcast_ana_${date}.csv
#iconv -f UTF-8 -c  -t GBK /home/likaiqing/shell/crawler_ana/crawler_report/crawler_day_broadcast_ana_${date}.csv > /home/likaiqing/shell/crawler_ana/crawler_report/crawler_day_broadcast_ana_${date}_gbk.csv
#rm -rf /home/likaiqing/shell/crawler_ana/crawler_report/crawler_day_broadcast_ana_${date}.csv

#/usr/local/jdk1.8.0_60/bin/java -jar $export2exceljar $date panda_result.crawler_week_broadcast_ana par_date,plat,lives,live_time_avg,pcu_avg,broadcast_days_avg,new_flag "日期,平台,开播数,平均开播时间(时每天),平均PCU,平均开播天数,是否新主播(1=Y;0=N;2=ALL)" /home/likaiqing/shell/crawler_ana/crawler_report/

#hive -e "
#set hive.cli.print.header=true;
#select par_date as \`日期\` ,plat as \`平台\`,lives as \`开播数\`,live_time_avg as \`平均开播时间(时每天)\`,pcu_avg as \`平均PCU()\`,broadcast_days_avg as \`平均开播天数\`,new_flag as \`是否新主播(1=Y 0=N 2=ALL)\` from panda_result.crawler_week_broadcast_ana where par_date='$date';
#" > /home/likaiqing/shell/crawler_ana/crawler_report/crawler_week_broadcast_ana_${date}.csv
#iconv -f UTF-8 -c  -t GBK /home/likaiqing/shell/crawler_ana/crawler_report/crawler_week_broadcast_ana_${date}.csv > /home/likaiqing/shell/crawler_ana/crawler_report/crawler_week_broadcast_ana_${date}_gbk.csv
#rm -rf /home/likaiqing/shell/crawler_ana/crawler_report/crawler_week_broadcast_ana_${date}.csv

#/usr/local/jdk1.8.0_60/bin/java -jar $export2exceljar $date panda_result.crawler_day_plat_ana par_date,plat,category,lives,pcu,pcu_rank,raise_anchors,followers_changed "日期,平台,分类,开播数,PCU,PCU排名,新增主播数,订阅数变化" /home/likaiqing/shell/crawler_ana/crawler_report/

#hive -e "
#set hive.cli.print.header=true;
#select par_date as \`日期\`,plat as \`平台\`,category as \`分类\`,lives as \`开播数\`,pcu_rank as \`PCU排名\`,raise_anchors as \`新增主播数\`,followers_changed as \`订阅数变化\` from panda_result.crawler_day_plat_ana where par_date='$date';
#" > /home/likaiqing/shell/crawler_ana/crawler_report/crawler_day_plat_ana_${date}.csv
#iconv -f UTF-8 -c  -t GBK /home/likaiqing/shell/crawler_ana/crawler_report/crawler_day_plat_ana_${date}.csv > /home/likaiqing/shell/crawler_ana/crawler_report/crawler_day_plat_ana_${date}_gbk.csv
#rm -rf /home/likaiqing/shell/crawler_ana/crawler_report/crawler_day_plat_ana_${date}.csv

#/usr/local/jdk1.8.0_60/bin/java -jar $export2exceljar $date panda_result.crawler_week_plat_ana par_date,plat,category,lives,pcu,pcu_rank,raise_anchors,followers_changed "日期,平台,分类,开播数,PCU,PCU排名,新增主播数,订阅数变化" /home/likaiqing/shell/crawler_ana/crawler_report/

#hive -e "
#set hive.cli.print.header=true;
#select par_date as \`日期\`,plat as \`平台\`,category as \`分类\`,lives as \`开播数\`,pcu_rank as \`PCU排名\`,raise_anchors as \`新增主播数\`,followers_changed as \`订阅数变化\` from panda_result.crawler_week_plat_ana where par_date='$date';
#" > /home/likaiqing/shell/crawler_ana/crawler_report/crawler_week_plat_ana_${date}.csv
#iconv -f UTF-8 -c  -t GBK /home/likaiqing/shell/crawler_ana/crawler_report/crawler_week_plat_ana_${date}.csv > /home/likaiqing/shell/crawler_ana/crawler_report/crawler_week_plat_ana_${date}_gbk.csv
#rm -rf /home/likaiqing/shell/crawler_ana/crawler_report/crawler_week_plat_ana_${date}.csv

#/usr/local/jdk1.8.0_60/bin/java -jar $export2exceljar $date panda_result.crawler_day_detail_anchor_ana par_date,rid,name,plat,category,weight_num,is_new,live_time,rec_times,rec_times_r,max_pcu,cate_pcu_r,plat_pcu_r,last_max_pcu,pcu_raised,cate_pcu_raise_r,max_followers,last_max_followers,followers_raised,cate_followers_raise_r,last_max_weight,weight_raised "日期,主播号,主播昵称,平台,分类,体重,是否新主播(1=Y;0=N),直播时长(时),推荐次数,推荐次数排名,PCU,按分类PCU排名,按平台PCU排名,上次PCU,PCU增长,PCU增长排名,订阅数,上次订阅数,订阅数变化,订阅数变化排名,上次体重,体重增值" /home/likaiqing/shell/crawler_ana/crawler_report/

#hive -e "
#set hive.cli.print.header=true;
#select par_date as \`日期\` ,rid as \`主播号\`,name as \`主播昵称\`,plat as \`平台\`,category as \`分类\`,weight_num as \`体重\`,is_new as \`是否新主播(1=Y 0=N)\`,live_time as \`直播时长(时)\`,rec_times as \`推荐次数\`,rec_times_r as \`推荐次数排名\`,max_pcu as PCU,cate_pcu_r as \`按分类PCU排名\`,plat_pcu_r as \`按平台PCU排名\`,last_max_pcu as \`上次PCU\`,pcu_raised as \`PCU增长\`,cate_pcu_raise_r as \`PCU增长排名\`,max_followers as \`订阅数\`,last_max_followers as \`上次关注数\`,followers_raised as \`订阅数变化\` , cate_followers_raise_r  as \`订阅数变化排名\`,last_max_weight as \`上次体重\`,weight_raised as \`体重增值\` from panda_result.crawler_day_detail_anchor_ana where par_date='$date';" > /home/likaiqing/shell/crawler_ana/crawler_report/crawler_day_detail_anchor_ana_${date}.csv
#iconv -f UTF-8 -c  -t GBK /home/likaiqing/shell/crawler_ana/crawler_report/crawler_day_detail_anchor_ana_${date}.csv > /home/likaiqing/shell/crawler_ana/crawler_report/crawler_day_detail_anchor_ana_${date}_gbk.csv
#rm -rf /home/likaiqing/shell/crawler_ana/crawler_report/crawler_day_detail_anchor_ana_${date}.csv

#/usr/local/jdk1.8.0_60/bin/java -jar $export2exceljar $date panda_result.crawler_week_detail_anchor_ana par_date,rid,name,plat,category,weight_num,pre_weight_date,is_new,live_days,live_time_per_day,rec_times,rec_times_r,max_pcu,cate_pcu_r,plat_pcu_r,pre_pcu_date,pcu_raised,cate_pcu_raise_r,max_followers,pre_followers_date,followers_raised,cate_followers_raise_r,weight_raised "日期,主播号,主播昵称,平台,分类,体重,上次体重日期,是否新主播(1=Y;0=N),直播天数,平均直播时长(时每天),推荐次数,推荐次数排名,PCU,按分类PCU排名,按平台PCU排名,本周第一个PCU日期,PCU增长,PCU增长排名,订阅数,本周第一次订阅日期,订阅数变化,订阅数变化排名,体重增值" /home/likaiqing/shell/crawler_ana/crawler_report/

#hive -e "
#set hive.cli.print.header=true;
#select par_date as \`日期\` ,rid as \`主播号\`,name as \`主播昵称\`,plat as \`平台\`,category as \`分类\`,weight_num as \`体重\`,pre_weight_date as \`上次体重日期\`,pre_weight as \`上次体重\`,is_new as \`新主播(1=Y 0=N)\`,live_days \`直播天数\`,live_time_per_day as \`平均直播时长(时每天)\`,rec_times as \`推荐次数\`,rec_times_r as \`推荐次数排名\`,max_pcu as PCU,cate_pcu_r as \`按分类PCU排名\`,plat_pcu_r as \`按平台PCU排名\`,pre_pcu_date \`本周第一个PCU日期\`,pcu_raised as \`PCU增长\`,cate_pcu_raise_r as \`PCU增长排名\`,max_followers as \`订阅数\`,pre_followers_date \`本周第一次订阅日期\`,followers_raised as \`订阅数变化\`,cate_followers_raise_r as \`订阅数变化排名\`,weight_raised as \`体重增值\` from panda_result.crawler_week_detail_anchor_ana where par_date='$date';" > /home/likaiqing/shell/crawler_ana/crawler_report/crawler_week_detail_anchor_ana_${date}.csv
#iconv -f UTF-8 -c  -t GBK /home/likaiqing/shell/crawler_ana/crawler_report/crawler_week_detail_anchor_ana_${date}.csv >  /home/likaiqing/shell/crawler_ana/crawler_report/crawler_week_detail_anchor_ana_${date}_gbk.csv
#rm -rf /home/likaiqing/shell/crawler_ana/crawler_report/crawler_week_detail_anchor_ana_${date}.csv
#rm -rf /home/likaiqing/shell/crawler_ana/crawler_ana_${date}.zip
#cd /home/likaiqing/shell/crawler_ana/
#zip -m /home/likaiqing/shell/crawler_ana/crawler_ana_${date}.zip ./crawler_report/*${date}.xlsx

#sh /home/likaiqing/shell/crawler_ana/mail_attach_report.sh $date
