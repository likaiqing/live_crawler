#!/bin/bash

date=$1
date=${date:=`date -d 'yesterday' +%Y%m%d`}
sub_1_days=`date -d "-1day $date" +%Y%m%d`
minutes=3
detail_minutes=15
douyu_detail_minutes=15
hive -e "
insert overwrite table panda_competitor.crawler_day_anchor_analyse partition(par_date)
SELECT
  p1.rid,
  p1.plat,
  category,
  max_pcu,
  live_times,
  duration,
  weight,
  followers,
  rec_times,
  CASE WHEN p2.rid IS NULL
    THEN 1
  ELSE 0 END is_new,
  '$date'
FROM
  (
    SELECT
      coalesce(ana.rid, rec.rid)              rid,
      coalesce(ana.plat, rec.plat)            plat,
      coalesce(ana.category, rec.category)    category,
      coalesce(ana.max_pcu, rec.max_pcu)      max_pcu,
      coalesce(ana.live_times, rec.rec_times) live_times,
      coalesce(ana.duration, rec.duration)    duration,
      coalesce(ana.weight, rec.weight)        weight,
      coalesce(ana.followers, rec.followers)  followers,
      coalesce(rec.rec_times, 0)              rec_times
    FROM
      (
        SELECT
          coalesce(dur.rid, pcu.rid)               rid,
          coalesce(dur.plat, pcu.plat)             plat,
          coalesce(dur.category, pcu.category)     category,
          coalesce(dur.live_times, pcu.live_times) live_times,
          coalesce(dur.duration, pcu.duration)     duration,
          coalesce(dur.max_pcu, pcu.max_pcu)       max_pcu,
          coalesce(pcu.weight, 0)                  weight,
          coalesce(pcu.followers, 0)               followers
        FROM
          (
            SELECT
              rid,
              split(task, 'anchor') [0]                       plat,
              category,
              max(populary_num)                               max_pcu,
              count(DISTINCT task_random)                     live_times,
              round(count(DISTINCT task_random) / 60 * $minutes, 2) duration
            FROM panda_competitor.crawler_anchor
            WHERE par_date = '$date' AND task LIKE '%anchor' and category !=''
            GROUP BY rid, split(task, 'anchor') [0], category
          ) dur
          FULL JOIN
          (
            SELECT
              rid,
              plat,
              category,
              live_times,
              CASE WHEN plat = 'douyu'
                THEN round(live_times / 60 * $douyu_detail_minutes, 2)
              ELSE round(live_times / 60 * $detail_minutes, 2) END duration,
              pcu                                     max_pcu,
              weight,
              followers
            FROM
              (
                SELECT
                  rid,
                  split(task, 'detailanchor') [0] plat,
                  category_sec                    category,
                  count(DISTINCT task_random)     live_times,
                  max(online_num)                 pcu,
            sum(case when z.rw=1 then weight_num else 0 end)weight,
          sum(case when z.rw=1 then follower_num else 0 end)followers
                FROM
                  (select a.*,row_number()over(partition by rid order by task_random desc)rw from panda_competitor.crawler_detail_anchor a WHERE par_date = '$date' AND task LIKE '%detailanchor' and category_sec !='')z
                GROUP BY rid, split(task, 'detailanchor') [0], category_sec
              ) d
          ) pcu
            ON dur.rid = pcu.rid AND dur.plat = pcu.plat AND dur.category = pcu.category
      ) ana
      FULL JOIN
      (
        SELECT
          rid,
          split(task, 'index') [0]                        plat,
          category_sec                                    category,
          count(DISTINCT task_random)                     rec_times,
          round(count(DISTINCT task_random) / 60 * $minutes, 2) duration,
          max(online_num)                                 max_pcu,
            sum(case when z.rw=1 then weight_num else 0 end)weight,
        sum(case when z.rw=1 then follower_num else 0 end)followers
        FROM 
          (select a.*,row_number()over(partition by rid order by task_random desc) rw from panda_competitor.crawler_indexrec_detail_anchor a WHERE par_date = '$date' AND task LIKE '%indexrec') z
        GROUP BY rid, split(task, 'index') [0], category_sec
      ) rec
        ON ana.rid = rec.rid AND ana.plat = rec.plat AND ana.category = rec.category
  ) p1
  LEFT JOIN
  (
    SELECT distinct rid,plat
    FROM panda_competitor.crawler_distinct_anchor
    WHERE par_date = '$sub_1_days'
  ) p2
    ON p1.rid = p2.rid and p1.plat=p2.plat;
"
