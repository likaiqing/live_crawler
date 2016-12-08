#!/bin/bash

date=$1
date=${date:=`date +%Y%m%d`}
hour=$2
hour=${hour:=`date +%H`}
hive -e "
insert overwrite table panda_result.panda_detail_anchor_crawler partition(par_date,hour)
SELECT
  rid,
  name,
  title,
  category_fir,
  category_sec,
  online_Num,
  follower_num,
  task,
  rank,
  weight_str,
  weight_num,
  tag,
  url,
  create_time,
  notice,
  last_start_time,
  task_random,
  par_date,
  hour
FROM
  (
    SELECT
      rid,
      regexp_replace(name,'\t| |,|\"','') name,
      regexp_replace(title,'\t| |,|\"','') title,
      regexp_replace(category_fir,'\t| |,|\"','') category_fir,
      regexp_replace(category_sec,'\t| |,|\"','') category_sec,
      online_Num,
      follower_num,
      task,
      rank,
      weight_str,
      weight_num,
      tag,
      url,
      create_time,
      regexp_replace(notice,'\t| |,|\"','') notice,
      last_start_time,
      task_random,
      par_date,
      hour,
      row_number()
      OVER (PARTITION BY par_date, hour, task, task_random, rid) r
    FROM
      panda_realtime.panda_detail_anchor_crawler
    WHERE par_date = '$date' AND hour='$hour'
          AND (task = 'douyudetailanchor' OR task = 'huyadetailanchor' or task='douyuindexrec' or task='huyaindexrec' or task='douyunewlive')
          AND category_sec != '' AND category_sec IS NOT NULL AND rid != '' AND rid IS NOT NULL and name !='' and name is not null
  ) r
WHERE r.r = 1;
"
