#!/bin/bash

date=$1
date=${date:=`date +%Y%m%d`}
hour=$2
hour=${hour:=`date +%H`}
hive -e "
insert overwrite table panda_result.panda_anchor_crawler partition(par_date,hour)
SELECT
  rid,
  name,
  title,
  category,
  populary_str,
  populary_num,
  task,
  plat,
  url_cate,
  create_time,
  url,
  task_random,
  par_date,
  hour
FROM
  (
    SELECT
      rid,
      regexp_replace(name,'\t| ','') name,
      regexp_replace(title,'\t| ','') title,
      regexp_replace(category,'\t| ','') category,
      populary_str,
      populary_num,
      task,
      plat,
      url_cate,
      create_time,
      url,
      task_random,
      par_date,
      hour,
      row_number()
      OVER (PARTITION BY par_date,hour,task,task_random,rid) r
    FROM panda_realtime.panda_anchor_crawler
    WHERE par_date = '$date' and hour='$hour' AND (task='chushouanchor' or task='douyuanchor' 
or task='huyaanchor' or task='longzhuanchor' or task='quanminanchor' or task='zhanqianchor') AND category != '' AND category IS NOT NULL and name !='' and name is not null
  ) r
WHERE r.r = 1 ;
"
