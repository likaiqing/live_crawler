#!/bin/bash

date=$1
date=${date:=`date -d 'yesterday' +%Y%m%d`}
sub_6_days=`date -d "-6 day ${date}" +%Y%m%d`
sub_7_days=`date -d "-7 day ${date}" +%Y%m%d`

hive -e "
insert overwrite table panda_result.crawler_week_broadcast_ana partition(par_date)
SELECT
  plat,
  count(DISTINCT rid)                                   lives,
  round((sum(live_times) / 4) / count(DISTINCT rid), 2) live_time_avg,
  sum(max_pcu) / count(DISTINCT rid)                    pcu_avg,
  round(sum(live_days) / count(DISTINCT rid),2)                  live_days_avg,
  2,
  '$date'
FROM
  (
    SELECT
      plat,
      rid,
      count(DISTINCT par_date)    live_days,
      count(DISTINCT task_random) live_times,
      max(populary_num)           max_pcu
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
        WHERE par_date BETWEEN '$sub_6_days' AND '$date' AND category != '' AND category IS NOT NULL
      ) format
    GROUP BY plat, rid
  ) agg
GROUP BY plat
UNION ALL
SELECT
  agg.plat,
  count(DISTINCT agg.rid)                               lives,
  round((sum(live_times) / 4) / count(DISTINCT agg.rid), 2) live_time_avg,
  sum(max_pcu) / count(DISTINCT agg.rid)                    pcu_avg,
  round(sum(live_days) / count(DISTINCT agg.rid),2)                  live_days_avg,
  CASE WHEN dis.rid IS NULL
    THEN 1
  ELSE 0 END                                            is_new,
  '$date'
FROM
  (
    SELECT
      plat,
      rid,
      count(DISTINCT par_date)    live_days,
      count(DISTINCT task_random) live_times,
      max(populary_num)           max_pcu
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
        WHERE par_date BETWEEN '$sub_6_days' AND '$date' AND category != '' AND category IS NOT NULL
      ) format
    GROUP BY plat, rid
  ) agg
  LEFT JOIN
  (
    SELECT
      rid,
      plat
    FROM panda_result.panda_distinct_anchor_crawler
    WHERE par_date = '$sub_7_days'
  ) dis
    ON agg.rid = dis.rid AND agg.plat = dis.plat
GROUP BY agg.plat,
  CASE WHEN dis.rid IS NULL
    THEN 1
  ELSE 0 END;
"
