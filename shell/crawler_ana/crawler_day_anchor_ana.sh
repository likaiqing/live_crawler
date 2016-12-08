#!/bin/bash

date=$1
date=${date:=`date -d 'yesterday' +%Y%m%d`}

hive -e "
insert overwrite table panda_result.crawler_day_anchor_ana partition(par_date)
SELECT
  rid,
  name,
  plat,
  category,
  0                        rec_times,
  max_pcu,
  max_pcu - min_pcu        day_pcu_raise,
  0                        day_fol_changed,
  round(live_times/2,2)          live_time,
  0                        is_new,
  rank()
  OVER (PARTITION BY par_date, plat, category
    ORDER BY max_pcu DESC) cate_pcu_rank,
  0                        fol_rank,
  0                        rec_times_rank,
 rank()
  OVER (PARTITION BY par_date, plat
    ORDER BY max_pcu DESC) plat_pcu_rank,
  par_date
FROM
  (
    SELECT
      par_date,
      rid,
      name,
      plat,
      category,
      max(populary_num)           max_pcu,
      min(populary_num)           min_pcu,
      count(DISTINCT task_random) live_times
    FROM
      (
        SELECT
          par_date,
          regexp_replace(rid, '/', '') rid,
          name,
          populary_num,
          plat,
          category,
          concat(substr(create_time, 0, 14),
                 CASE WHEN substr(create_time, 15, 2) < 30
                   THEN 05
                 WHEN substr(create_time, 15, 2) > 30
                   THEN 35 END)        task_random
        FROM
          panda_result.panda_anchor_crawler
        WHERE par_date ='$date' AND category != '' AND category IS NOT NULL
      ) anc_crawler
    GROUP BY par_date, rid, name, plat, category
    HAVING max(populary_num) > 500
  ) group_tmp;
"
