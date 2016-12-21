#!/bin/bash

date=$1
date=${date:=`date -d 'yesterday' +%Y%m%d`}
sub_1_days=`date -d "-1day $date" +%Y%m%d`

hive -e "
insert overwrite table panda_competitor.crawler_day_plat_analyse partition(par_date)
SELECT
  coalesce(ana.plat, rec.plat)            plat,
  coalesce(ana.max_pcu, rec.max_pcu)      max_pcu,
  coalesce(ana.live_times, rec.rec_times) live_times,
  coalesce(ana.duration, rec.duration)    duraion,
  coalesce(ana.weight, rec.weight)        weight,
  coalesce(ana.followers, rec.followers)  followers,
  coalesce(rec.rec_times, 0)              rec_times,
  coalesce(ana.lives, rec.lives)          lives,
  new_anchors.new_anchors,
  '$date'
FROM
  (
    SELECT
      coalesce(dur.plat, pcu.plat)             plat,
      coalesce(dur.live_times, pcu.live_times) live_times,
      coalesce(dur.duration, pcu.duration)     duration,
      coalesce(dur.max_pcu, pcu.max_pcu)       max_pcu,
      coalesce(pcu.weight, 0)                  weight,
      coalesce(pcu.followers, 0)               followers,
      coalesce(dur.lives, pcu.lives)           lives
    FROM
      (
        SELECT
          plat,
          sum(live_times)                    live_times,
          sum(lives)                      lives,
          round(sum(live_times) / 60 * 5, 2) duration,
          max(pcu)                        max_pcu
        FROM
          (
            SELECT
              task_random,
              split(task, 'anchor') [0]       plat,
              category,
              count(DISTINCT rid) live_times,
              count(DISTINCT rid)             lives,
              sum(populary_num)               pcu
            FROM panda_competitor.crawler_anchor
            WHERE par_date = '$date' AND task LIKE '%anchor'
            GROUP BY task_random, split(task, 'anchor') [0], category
          ) t_r
        GROUP BY plat
      ) dur
      FULL JOIN
      (
        SELECT
          plat,
          sum(anchors)                             live_times,
          CASE WHEN plat = 'douyu'
            THEN round(sum(anchors) / 60, 2)
          ELSE round(sum(anchors) / 60 * 5, 2) END duration,
          sum(lives)                               lives,
          max(pcu)                                 max_pcu,
          max(weight)                              weight,
          max(followers)                           followers
        FROM
          (
            SELECT
              task_random,
              split(task, 'detailanchor') [0] plat,
              category_sec                    category,
              count(DISTINCT rid,task_random) anchors,
              count(DISTINCT rid)             lives,
              sum(online_num)                 pcu,
              sum(weight_num)                 weight,
              sum(follower_num)               followers
            FROM
              panda_competitor.crawler_detail_anchor
            WHERE par_date = '$date' AND task LIKE '%detailanchor'
            GROUP BY task_random, split(task, 'detailanchor') [0], category_sec
          ) d
        GROUP BY plat
      ) pcu
        ON dur.plat = pcu.plat
  ) ana
  FULL JOIN
  (
    SELECT
      plat,
      sum(anchors)                    rec_times,
      round(sum(anchors) / 60 * 5, 2) duration,
      sum(lives)                      lives,
      max(max_pcu)                    max_pcu,
      max(weight)                     weight,
      max(followers)                  followers
    FROM
      (
        SELECT
          task_random,
          split(task, 'index') [0]        plat,
          category_sec                    category,
          count(DISTINCT rid,task_random) anchors,
          count(DISTINCT rid)             lives,
          max(online_num)                 max_pcu,
          max(weight_num)                 weight,
          max(follower_num)               followers
        FROM
          panda_competitor.crawler_indexrec_detail_anchor
        WHERE par_date = '$date' AND task LIKE '%indexrec'
        GROUP BY task_random, split(task, 'index') [0], category_sec
      ) rec_tr
    GROUP BY plat
  ) rec
    ON ana.plat = rec.plat
  LEFT JOIN
  (
    SELECT
      dis1.plat,
      count(1) new_anchors
    FROM
      (
        SELECT
          DISTINCT
          plat,
          rid
        FROM
          (
            SELECT
              DISTINCT
              split(task, 'anchor') [0] plat,
              rid
            FROM panda_competitor.crawler_anchor
            WHERE par_date = '$date' AND task LIKE '%anchor'
            UNION ALL
            SELECT
              DISTINCT
              split(task, 'detailanchor') [0] plat,
              rid
            FROM panda_competitor.crawler_detail_anchor
            WHERE par_date = '$date' AND task LIKE '%detailanchor'
            UNION ALL
            SELECT
              DISTINCT
              split(task, 'index') [0] plat,
              rid
            FROM
              panda_competitor.crawler_indexrec_detail_anchor
            WHERE par_date = '$date' AND task LIKE '%indexrec'
          ) a
      ) dis1
      LEFT JOIN
      (
        SELECT
          DISTINCT
          rid,
          plat
        FROM panda_competitor.crawler_distinct_anchor
        WHERE par_date = '$sub_1_days'
      ) dis2
        ON dis1.plat = dis2.plat AND dis1.rid = dis2.rid
    WHERE dis2.rid IS NULL
    GROUP BY dis1.plat
  ) new_anchors
    ON ana.plat = new_anchors.plat;
"