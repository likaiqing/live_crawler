#!/bin/bash

date=$1
date=${date:=`date -d 'yesterday' +%Y%m%d`}
sub_1_days=`date -d "-1day $date" +%Y%m%d`

hive -e "
insert overwrite table panda_result.crawler_day_broadcast_ana partition(par_date)
SELECT
  plat,
  count(DISTINCT rid)                        lives,
  round((sum(live_times) /4) / count(DISTINCT rid),2) live_time_avg,
  sum(max_pcu) / count(DISTINCT rid)         pcu_avf,
  2,
  par_date
FROM
  (
    SELECT
      par_date,
      plat,
      rid,
      max(populary_num)           max_pcu,
      count(DISTINCT task_random) live_times
    FROM
      (
        SELECT
          par_date,
          regexp_replace(rid, '/', '') rid,
          name,
          populary_num,
          plat,
          concat(substr(create_time, 0, 14),
                 CASE WHEN substr(create_time, 15, 2) < 30
                   THEN 05
                 WHEN substr(create_time, 15, 2) > 30
                   THEN 35 END)        task_random
        FROM
          panda_result.panda_anchor_crawler
        WHERE par_date ='$date'
      ) anc_crawler
    GROUP BY par_date, rid, plat
  ) r
GROUP BY par_date, plat
UNION ALL
SELECT
  plat,
  count(DISTINCT rid)                        lives,
  round((sum(live_times) /4) / count(DISTINCT rid),2) live_time_avg,
  sum(max_pcu) / count(DISTINCT rid)         pcu_avf,
  is_new,
  par_date
FROM
  (
    SELECT
      par_date,
      anc_crawler.plat,
      anc_crawler.rid,
      CASE WHEN dis.rid IS NULL
        THEN 1
      ELSE 0 END                  is_new,
      max(populary_num)           max_pcu,
      count(DISTINCT task_random) live_times
    FROM
      (
        SELECT
          par_date,
          rid,
          name,
          populary_num,
          plat,
          concat(substr(create_time, 0, 14),
                 CASE WHEN substr(create_time, 15, 2) < 30
                   THEN 05
                 WHEN substr(create_time, 15, 2) > 30
                   THEN 35 END) task_random
        FROM
          panda_result.panda_anchor_crawler
        WHERE par_date = '$date'
      ) anc_crawler
      LEFT JOIN
      (
        SELECT
          rid,
          plat
        FROM panda_result.panda_distinct_anchor_crawler
        WHERE par_date = '$sub_1_days'
      ) dis
        ON anc_crawler.rid = dis.rid AND anc_crawler.plat = dis.plat
    GROUP BY par_date, anc_crawler.plat, anc_crawler.rid,
      CASE WHEN dis.rid IS NULL
        THEN 1
      ELSE 0 END
  ) day_bro
GROUP BY par_date,plat,is_new;
"

