#!/bin/bash

date=$1
date=${date:=`date -d 'yesterday' +%Y%m%d`}
hive -e "
insert overwrite table panda_competitor.crawler_day_anchor_analyse partition(par_date)
SELECT
  coalesce(ana.rid, rec.rid)              rid,
  coalesce(ana.plat, rec.plat)            plat,
  coalesce(ana.category, rec.category)    category,
  coalesce(ana.max_pcu, rec.max_pcu)              max_pcu,
  coalesce(ana.live_times, rec.rec_times) live_times,
  coalesce(ana.duration, rec.duration)    duraion,
  coalesce(ana.weight, rec.weight)        weight,
  coalesce(ana.followers, rec.followers)  followers,
  coalesce(rec.rec_times, 0)              rec_times,
  '$date'
FROM
  (
    SELECT
      coalesce(dur.rid, pcu.rid)               rid,
      coalesce(dur.plat, pcu.plat)             plat,
      coalesce(dur.category, pcu.category)     category,
      coalesce(dur.live_times, pcu.live_times) live_times,
      coalesce(dur.duration, pcu.duration)     duration,
      coalesce(dur.max_pcu, pcu.max_pcu)               max_pcu,
      coalesce(pcu.weight, 0)                  weight,
      coalesce(pcu.followers, 0)               followers
    FROM
      (
        SELECT
          rid,
          split(task, 'anchor') [0]             plat,
          category,
          max(populary_num)                     max_pcu,
          count(DISTINCT task_random)           live_times,
          round(count(DISTINCT task_random) / 60 * 15,2) duration
        FROM panda_competitor.crawler_anchor
        WHERE par_date = '$date' and task like '%anchor'
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
            THEN round(live_times / 60,2)
          ELSE round(live_times / 60 * 15,2) END duration,
          pcu max_pcu,
          weight,
          followers
        FROM
          (
            SELECT
              rid,
              split(task, 'detailanchor') [0]   plat,
              category_sec                category,
              count(DISTINCT task_random) live_times,
              max(online_num)             pcu,
              max(weight_num)             weight,
              max(follower_num)           followers
            FROM
              panda_competitor.crawler_detail_anchor
            WHERE par_date = '$date' and task like '%detailanchor'
            GROUP BY rid, split(task, 'detailanchor') [0], category_sec
          ) d
      ) pcu
        ON dur.rid = pcu.rid AND dur.plat = pcu.plat AND dur.category = pcu.category
  ) ana
  FULL JOIN
  (
    SELECT
      rid,
      split(task, 'index') [0]              plat,
      category_sec category,
      count(DISTINCT task_random)           rec_times,
      round(count(DISTINCT task_random) / 60 * 15,2) duration,
      max(online_num)                       max_pcu,
      max(weight_num)                       weight,
      max(follower_num)                     followers
    FROM
      panda_competitor.crawler_indexrec_detail_anchor
    WHERE par_date = '$date' and task like '%indexrec'
    GROUP BY rid, split(task, 'index') [0], category_sec
  ) rec
    ON ana.rid = rec.rid AND ana.plat = rec.plat AND ana.category = rec.category;
"