#!/bin/bash

date=$1
date=${date:=`date -d 'yesterday' +%Y%m%d`}

export2exceljar=/home/likaiqing/hive-tool/export2excel.jar
mailjar=/home/likaiqing/hive-tool/mail_attach.jar

exportdir=/home/likaiqing/shell/crawler_ana/competitor_report/

/usr/bin/java -jar $export2exceljar $date panda_competitor_result.plat_day_report par_date,plat_id,plat_name,pcu,lives,max_lives,lives_changed,followers,followers_changed,weight,weight_changed,categories,new_categories,reduce_categories "日期,平台ID,平台名称,PCU,直播数,同时最大直播数,直播数变化,订阅数,订阅数变化,体重(g),体重变化数(g),板块数,新板块数,减少板块数" $exportdir

/usr/bin/java -jar $export2exceljar $date panda_competitor_result.plat_day_change_report par_date,plat_id,plat_name,lives,lives_changed_rank_change,followers,followers_changed_rank_change "日期,平台ID,平台名称,直播数,直播数增减位订阅数,订阅数,订阅数增减位" $exportdir

/usr/bin/java -jar $export2exceljar $date panda_competitor_result.category_day_report par_date,plat_id,plat_name,category,lives,lives_changed,pcu,followers,followers_changed,weight,weight_changed,is_new,duration "日期,平台ID,平台名称,板块名称,直播数,直播数变化,PCU,订阅数,订阅数变化,体重(g),体重变化数(g),是否新板块(1是0否),直播时长" $exportdir

/usr/bin/java -jar $export2exceljar $date panda_competitor_result.category_day_change_report par_date,plat_id,plat_name,category,lives,lives_changed_rank_change,followers,followers_changed_rank_change "日期,平台ID,平台名称,板块mingc,直播数,直播数增减位订阅数,订阅数,订阅数增减位" $exportdir

#主播PCU排行
/usr/bin/java -jar $export2exceljar $date panda_competitor_result.anchor_pcu_rank par_date,plat_id,plat,rid,name,rank,pcu,weight,fol,rank_changed,room_content "日期,平台ID,平台名称,主播ID,主播,排名,PCU,体重,订阅,名次变化,房间内容" /home/likaiqing/shell/crawler_ana/competitor_report/

#主播变化趋势增减表
/usr/bin/java -jar $export2exceljar $date panda_competitor_result.anchor_changed_rank par_date,plat_id,plat,rid,name,catrgory,pcu,pcu_changed,livetime,livetime_changed,fol,weight,weight_changed "日期,平台ID,平台名称,主播ID,主播,所属版区,Pcu,pcu增减数变化,开播时长,开播时长增减位数,订阅数,体重,体重增减位数" /home/likaiqing/shell/crawler_ana/competitor_report/

#主播订阅排行表：
/usr/bin/java -jar $export2exceljar $date panda_competitor_result.anchor_fol_rank par_date,plat_id,plat,rid,name,rank,pcu,weight,finnal_fol,fol_up,room_content "日期,平台ID,平台名称,主播ID,主播昵称,排名,PCU,体重,最终订阅数,本日订阅增长,房间内容" /home/likaiqing/shell/crawler_ana/competitor_report/

#主播订阅增长排行表：
/usr/bin/java -jar $export2exceljar $date panda_competitor_result.anchor_fol_up_rank par_date,plat_id,plat,rid,name,rank,pcu,weight,finnal_fol,fol_up,room_content "日期,平台ID,平台名称,主播ID,主播昵称,排名,PCU,体重,最终订阅数,本日订阅增长,房间内容" /home/likaiqing/shell/crawler_ana/competitor_report/

#主播体重增长排行表：
/usr/bin/java -jar $export2exceljar $date panda_competitor_result.anchor_weight_rank par_date,plat_id,plat,rid,name,rank,pcu,fol,weight_first,weight_final,room_content "日期,平台ID,平台名称,主播ID,主播昵称,排名,PCU,订阅数,初始体重,最终体重,房间内容" /home/likaiqing/shell/crawler_ana/competitor_report/

#主播播放时长排行表
/usr/bin/java -jar $export2exceljar $date panda_competitor_result.anchor_livetime_rank par_date,plat_id,plat,rid,name,rank,pcu,fol,livetime,room_content "日期,平台ID,平台名称,主播ID,主播昵称,排名,PCU,订阅数,直播时长,房间内容" /home/likaiqing/shell/crawler_ana/competitor_report/

#推荐味日表
/usr/bin/java -jar $export2exceljar $date panda_competitor_result.indexrec_day_report par_date,plat_id,plat_name,rid,name,url,pcu,followers,followers_changed,weight_changed,rec_duration "日期,平台ID,平台名称,推荐主播ID,推荐主播昵称,房间地址,PCU,订阅数,订阅增长,体重增长,推荐时长" /home/likaiqing/shell/crawler_ana/competitor_report/

#主播当天变化量
/usr/bin/java -jar $export2exceljar $date panda_competitor_result.anchor_day_changed_analyse_by_sameday par_date,rid,name,plat,category,pcu,last_followers,last_weight,followers_changed,weight_changed,title "日期,主播ID,主播昵称,平台名称,版区名称,PCU,订阅数,体重,订阅增长,体重增长,房间内容" /home/likaiqing/shell/crawler_ana/competitor_report/

#版区当天变化量
/usr/bin/java -jar $export2exceljar $date panda_competitor_result.category_day_changed_analyse_by_sameday par_date,plat,category,pcu,last_followers,last_weight,followers_changed,weight_changed "日期,平台名称,版区名称,PCU,订阅数,体重,订阅增长,体重增长" /home/likaiqing/shell/crawler_ana/competitor_report/

#平台当天变化量
/usr/bin/java -jar $export2exceljar $date panda_competitor_result.plat_day_changed_analyse_by_sameday par_date,plat,pcu,last_followers,last_weight,followers_changed,weight_changed "日期,平台名称,PCU,订阅数,体重,订阅增长,体重增长" /home/likaiqing/shell/crawler_ana/competitor_report/


zip_dir=/home/likaiqing/shell/crawler_ana
rm -rf $zip_dir/competitor_report_${date}.zip
cd $zip_dir
zip -m $zip_dir/competitor_report_${date}.zip ./competitor_report/*${date}.xlsx

/usr/bin/java -jar $mailjar "竞品分析:$date" "报表见附件" $zip_dir/competitor_report_${date}.zip "fengwenbo@panda.tv" "baimuhai@panda.tv,lushenggang@panda.tv,likaiqing@panda.tv,zhaolirong@panda.tv"
