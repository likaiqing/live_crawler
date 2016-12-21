#!/bin/bash

date=$1
date=${date:=`date -d 'yesterday' +%Y%m%d`}
sub_1_days=`date -d "-1day $date" +%Y%m%d`

hive -e "
insert overwrite table panda_competitor.crawler_day_cate_analyse partition(par_date)
SELECT
  coalesce(ana.plat, rec.plat)            plat,
  coalesce(ana.category, rec.category)    category,
  coalesce(ana.max_pcu, rec.max_pcu)      max_pcu,
  coalesce(ana.live_times, rec.rec_times) live_times,
  coalesce(ana.duration, rec.duration)    duraion,
  coalesce(ana.weight, rec.weight)        weight,
  coalesce(ana.followers, rec.followers)  followers,
  coalesce(rec.rec_times, 0)              rec_times,
  coalesce(cate.is_new, 1)                is_new,
  coalesce(cate.is_closed, 0)             is_closed,
  anchors.lives          lives,
  anchors.new_anchors,
  '$date'
FROM
  (
    SELECT
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
          plat,
          category,
          sum(anchors)                    live_times,
          round(sum(anchors) / 60 * 5, 2) duration,
          max(pcu)                        max_pcu
        FROM
          (
            SELECT
              task_random,
              split(task, 'anchor') [0]       plat,
              category,
              count(DISTINCT rid,task_random) anchors,
              sum(populary_num)               pcu
            FROM panda_competitor.crawler_anchor
            WHERE par_date = '$date' AND task LIKE '%anchor'
            GROUP BY task_random, split(task, 'anchor') [0], category
          ) t_r
        GROUP BY plat, category
      ) dur
      FULL JOIN
      (
        SELECT
          plat,
          category,
          sum(anchors)                             live_times,
          CASE WHEN plat = 'douyu'
            THEN round(sum(anchors) / 60, 2)
          ELSE round(sum(anchors) / 60 * 5, 2) END duration,
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
              sum(online_num)                 pcu,
              sum(weight_num)                 weight,
              sum(follower_num)               followers
            FROM
              panda_competitor.crawler_detail_anchor
            WHERE par_date = '$date' AND task LIKE '%detailanchor'
            GROUP BY task_random, split(task, 'detailanchor') [0], category_sec
          ) d
        GROUP BY plat, category
      ) pcu
        ON dur.plat = pcu.plat AND dur.category = pcu.category
  ) ana
  FULL JOIN
  (
    SELECT
      plat,
      category,
      sum(anchors)                    rec_times,
      round(sum(anchors) / 60 * 5, 2) duration,
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
          max(online_num)                 max_pcu,
          max(weight_num)                 weight,
          max(follower_num)               followers
        FROM
          panda_competitor.crawler_indexrec_detail_anchor
        WHERE par_date = '$date' AND task LIKE '%indexrec'
        GROUP BY task_random, split(task, 'index') [0], category_sec
      ) rec_tr
    GROUP BY plat, category
  ) rec
    ON ana.plat = rec.plat AND ana.category = rec.category
  FULL JOIN
  (
    SELECT
      coalesce(cate1.plat_name, cate2.plat_name) plat_name,
      coalesce(cate1.c_name, cate2.c_name)       c_name,
      CASE WHEN cate2.c_name IS NULL
        THEN 1
      ELSE 0 END                                 is_new,
      CASE WHEN cate1.c_name IS NULL
        THEN 1
      ELSE 0 END                                 is_closed
    FROM
      (
        SELECT
          DISTINCT
          plat_name,
          c_name
        FROM panda_competitor.crawler_category
        WHERE par_date = '$date'
      ) cate1
      FULL JOIN
      (
        SELECT
          DISTINCT
          plat_name,
          c_name
        FROM panda_competitor.crawler_category
        WHERE par_date = '$sub_1_days'
      ) cate2
        ON cate1.plat_name = cate2.plat_name AND cate1.c_name = cate2.c_name
  ) cate
    ON ana.plat = cate.plat_name AND ana.category = cate.c_name
  LEFT JOIN
  (
    SELECT
      dis1.plat,
      dis1.category,
      count(1) lives,
      sum(CASE WHEN dis2.rid IS NULL THEN 1 ELSE 0 end) new_anchors
    FROM
      (
        SELECT
          DISTINCT
          plat,
          category,
          rid
        FROM
          (
            SELECT
              DISTINCT
              split(task, 'anchor') [0] plat,
              category,
              rid
            FROM panda_competitor.crawler_anchor
            WHERE par_date = '$date' AND task LIKE '%anchor'
            UNION ALL
            SELECT
              DISTINCT
              split(task, 'detailanchor') [0] plat,
              category_sec,
              rid
            FROM panda_competitor.crawler_detail_anchor
            WHERE par_date = '$date' AND task LIKE '%detailanchor'
            UNION ALL
            SELECT
              DISTINCT
              split(task, 'index') [0] plat,
              category_sec,
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
    GROUP BY dis1.plat,dis1.category
  ) anchors
    ON ana.plat = anchors.plat AND ana.category=anchors.category;
"