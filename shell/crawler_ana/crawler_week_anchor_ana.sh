#!/bin/bash

date=$1
date=${date:=`date -d 'yesterday' +%Y%m%d`}
six_day_ago=`date -d "-6 day ${date}" +%Y%m%d`
hive -e "
insert overwrite table panda_result.crawler_week_anchor_ana partition(par_date)
SELECT
  week.rid,
  week.name,
  week.plat,
  week.category,
  0 rec_times,
  week.week_pcu                                  week_max_pcu,
  (last_max_pcu.max_pcu - first_max_pcu.max_pcu) week_pcu_raise,
  0 week_new_follow,
  live_time_avg,
  0 is_new_anchor,
  week_pcu_rank,
  0 week_fol_raise_rank,
  0 week_rec_rank,
  '$date'
FROM
  (
    SELECT
      rid,
      name,
      plat,
      category,
      round((live_times/4) / live_days, 2) live_time_avg,
      max_pcu                               week_pcu,
      round(week_sum_pcu / live_days, 2)    week_avg_pcu,
      rank()
      OVER (PARTITION BY plat, category
        ORDER BY max_pcu DESC)              week_pcu_rank,
      week_sum_pcu
    FROM
      (
        SELECT
          rid,
          name,
          plat,
          category,
          sum(max_pcu)             week_sum_pcu,
          max(max_pcu)             max_pcu,
          sum(live_times)          live_times,
          count(DISTINCT par_date) live_days
        FROM
          (
            SELECT
              par_date,
              rid,
              name,
              plat,
              category,
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
                  category,
                  concat(substr(create_time, 0, 14),
                         CASE WHEN substr(create_time, 15, 2) < 30
                           THEN 05
                         WHEN substr(create_time, 15, 2) > 30
                           THEN 35 END)        task_random
                FROM
                  panda_competitor.crawler_anchor
                WHERE par_date BETWEEN '$six_day_ago' AND '$date' AND category != '' AND category IS NOT NULL
              ) anc_crawler
            GROUP BY par_date, rid, name, plat, category
          ) group_tmp
        GROUP BY rid, name, plat, category
      ) week
  ) week
  LEFT JOIN
  (
    SELECT
      rid,
      name,
      plat,
      category,
      max_pcu
    FROM
      (
        SELECT
          rid,
          name,
          plat,
          category,
          max_pcu,
          row_number()
          OVER (PARTITION BY rid, name, plat, category
            ORDER BY par_date DESC) r
        FROM
          (
            SELECT
              par_date,
              regexp_replace(rid, '/', '') rid,
              name,
              plat,
              category,
              max(populary_num)            max_pcu
            FROM
              panda_competitor.crawler_anchor
            WHERE par_date BETWEEN '$six_day_ago' AND '$date' AND category != '' AND category IS NOT NULL
            GROUP BY par_date, rid, name, plat, category
          ) last
      ) r
    WHERE r.r = 1
  ) last_max_pcu
    ON week.rid = last_max_pcu.rid AND
       week.name = last_max_pcu.name AND
       week.plat = last_max_pcu.plat AND
       week.category = last_max_pcu.category
  LEFT JOIN
  (
    SELECT
      rid,
      name,
      plat,
      category,
      max_pcu
    FROM
      (
        SELECT
          rid,
          name,
          plat,
          category,
          max_pcu,
          row_number()
          OVER (PARTITION BY rid, name, plat, category
            ORDER BY par_date ASC) r
        FROM
          (
            SELECT
              par_date,
              regexp_replace(rid, '/', '') rid,
              name,
              plat,
              category,
              max(populary_num)            max_pcu
            FROM
              panda_competitor.crawler_anchor
            WHERE par_date BETWEEN '$six_day_ago' AND '$date' AND category != '' AND category IS NOT NULL
            GROUP BY par_date, rid, name, plat, category
          ) last
      ) r
    WHERE r.r = 1
  ) first_max_pcu
    ON week.rid = first_max_pcu.rid AND
       week.name = first_max_pcu.name AND
       week.plat = first_max_pcu.plat AND
       week.category = first_max_pcu.category;
"
