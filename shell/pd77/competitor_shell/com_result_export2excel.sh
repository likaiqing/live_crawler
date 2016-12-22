#!/bin/bash

date=$1
date=${date:=`date -d 'yesterday' +%Y%m%d`}

export2exceljar=/home/likaiqing/hive-tool/export2excel.jar
mailjar=/home/likaiqing/hive-tool/mail_attach.jar

exportdir=/home/likaiqing/shell/crawler_ana/competitor_report/

/usr/bin/java -jar $export2exceljar $date panda_competitor_result.plat_day_report par_date,plat_id,plat_name,pcu,lives,max_lives,lives_changed,followers,followers_changed,weight,weight_changed,categories,new_categories,reduce_categories "日期,平台ID,平台名称,PCU,直播数,同时最大直播数,直播数变化,订阅数,订阅数变化,体重(g),体重变化数(g),板块数,新板块数,减少板块数" $exportdir

/usr/bin/java -jar $export2exceljar $date panda_competitor_result.plat_day_change_report par_date,plat_id,plat_name,lives,lives_changed_rank_change,followers,followers_changed_rank_change "日期,平台ID,平台名称,直播数,直播数增减位订阅数,订阅数增减位" $exportdir

/usr/bin/java -jar $export2exceljar $date panda_competitor_result.category_day_report par_date,plat_id,plat_name,category,lives,lives_changed,followers,followers_changed,weight,weight_changed,is_new,duration "日期,平台ID,平台名称,板块名称,直播数,直播数变化,订阅数,订阅数变化,体重(g),体重变化数(g),是否新板块(1是0否),直播时长" $exportdir

/usr/bin/java -jar $export2exceljar $date panda_competitor_result.category_day_change_report par_date,plat_id,plat_name,category,lives,lives_changed_rank_change,followers,followers_changed_rank_change "日期,平台ID,平台名称,板块mingc,直播数,直播数增减位订阅数,订阅数增减位" $exportdir




zip_dir=/home/likaiqing/shell/crawler_ana
rm -rf $zip_dir/competitor_report_${date}.zip
cd $exportdir
zip -m $zip_dir/competitor_report_${date}.zip ./competitor_report/*${date}.xlsx

/usr/bin/java -jar $mailjar "竞品分析:$date" "报表见附件" $zip_dir/competitor_report_${date}.zip
