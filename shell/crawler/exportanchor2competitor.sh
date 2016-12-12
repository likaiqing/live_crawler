#!/bin/bash

date=$1
date=${date:=`date -d 'yesterday' +%Y%m%d`}

hive -e "
insert overwrite table panda_competitor.panda_douyu_anchor partition(par_date) select rid,name,title,category,populary_str,populary_num,task,plat,url_cate,create_time,url,task_random,hour,par_date from panda_result.panda_anchor_crawler where par_date='$date' and task='douyuanchor'; "

hive -e "
insert overwrite table panda_competitor.panda_huya_anchor partition(par_date) select rid,name,title,category,populary_str,populary_num,task,plat,url_cate,create_time,url,task_random,hour,par_date from panda_result.panda_anchor_crawler where par_date='$date' and task='huyaanchor'; "

hive -e "
insert overwrite table panda_competitor.panda_longzhu_anchor partition(par_date) select rid,name,title,category,populary_str,populary_num,task,plat,url_cate,create_time,url,task_random,hour,par_date from panda_result.panda_anchor_crawler where par_date='$date' and task='longzhuanchor'; "

hive -e "
insert overwrite table panda_competitor.panda_zhanqi_anchor partition(par_date) select rid,name,title,category,populary_str,populary_num,task,plat,url_cate,create_time,url,task_random,hour,par_date from panda_result.panda_anchor_crawler where par_date='$date' and task='zhanqianchor'; "

hive -e "
insert overwrite table panda_competitor.panda_quanmin_anchor partition(par_date) select rid,name,title,category,populary_str,populary_num,task,plat,url_cate,create_time,url,task_random,hour,par_date from panda_result.panda_anchor_crawler where par_date='$date' and task='quanminanchor'; "

hive -e "
insert overwrite table panda_competitor.panda_chushou_anchor partition(par_date) select rid,name,title,category,populary_str,populary_num,task,plat,url_cate,create_time,url,task_random,hour,par_date from panda_result.panda_anchor_crawler where par_date='$date' and task='chushouanchor'; "




hive -e "
insert overwrite table panda_competitor.panda_douyu_detail_anchor partition(par_date) select rid,name,title,category_fir,category_sec,online_num,follower_num,task,rank,weight_str,weight_num,tag,url,create_time,notice,last_start_time,task_random,hour,par_date from panda_result.panda_detail_anchor_crawler where par_date='$date' and task='douyudetailanchor'; "


hive -e "
insert overwrite table panda_competitor.panda_huya_detail_anchor partition(par_date) select rid,name,title,category_fir,category_sec,online_num,follower_num,task,rank,weight_str,weight_num,tag,url,create_time,notice,last_start_time,task_random,hour,par_date from panda_result.panda_detail_anchor_crawler where par_date='$date' and task='huyadetailanchor'; "

hive -e "
insert overwrite table panda_competitor.panda_douyunewlive_detail_anchor partition(par_date) select rid,name,title,category_fir,category_sec,online_num,follower_num,task,rank,weight_str,weight_num,tag,url,create_time,notice,last_start_time,task_random,hour,par_date from panda_result.panda_detail_anchor_crawler where par_date='$date' and task='douyunewlive'; "

hive -e "
insert overwrite table panda_competitor.panda_douyuindexrec_detail_anchor partition(par_date) select rid,name,title,category_fir,category_sec,online_num,follower_num,task,rank,weight_str,weight_num,tag,url,create_time,notice,last_start_time,task_random,hour,par_date from panda_result.panda_detail_anchor_crawler where par_date='$date' and task='douyuindexrec'; "

hive -e "
insert overwrite table panda_competitor.panda_huyaindexrec_detail_anchor partition(par_date) select rid,name,title,category_fir,category_sec,online_num,follower_num,task,rank,weight_str,weight_num,tag,url,create_time,notice,last_start_time,task_random,hour,par_date from panda_result.panda_detail_anchor_crawler where par_date='$date' and task='huyaindexrec'; "

sh /home/likaiqing/shell/crawler/competitor_distinct.sh $date
