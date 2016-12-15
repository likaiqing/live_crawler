#!/bin/bash

date=$1
date=${date:=`date -d 'yesterday' +%Y%m%d`}

hive -e "
insert overwrite table panda_result.crawler_day_plat_ana partition(par_date)
SELECT
  plat,
  category,
  lives,
  rank()
  OVER (PARTITION BY plat
    ORDER BY max_pcu DESC) pcu_rank,
  0 raise_anchors,
  0 follower_changed,
  max_pcu,
  '$date'
FROM
  (
    SELECT
      par_date,
      plat,
      category,
      count(DISTINCT rid) lives,
      max(populary_num)   max_pcu
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
          panda_competitor.crawler_anchor
        WHERE par_date = '$date' AND category != '' AND category IS NOT NULL
      ) anc_crawler
    GROUP BY par_date, plat, category
  ) agg;
  "
