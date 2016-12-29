#!/bin/bash

date=$1
date=${date:=`date -d 'yesterday' +%Y%m%d`}
sub_1_days=`date -d "-1day $date" +%Y%m%d`
minutes=15
hive -e "
insert overwrite table panda_competitor.crawler_day_cate_analyse partition(par_date)
SELECT
  cate1.plat,
  cate1.category,
  coalesce(t.max_pcu, 0)           max_pcu,
  coalesce(t.live_times, 0)        live_times,
  coalesce(t.duration, 0.0)        duraion,
  coalesce(t.weight, 0)            weight,
  coalesce(t.followers, 0)         followers,
  coalesce(t.rec_times, 0)         rec_times,
  cate1.is_new,
  cate1.is_closed,
  coalesce(anchors.lives, 0)       lives,
  coalesce(anchors.new_anchors, 0) new_anchors,
  '$date'
FROM
  (
    SELECT
      cate1.plat,
      cate1.category,
      cate_all.is_new,
      cate_all.is_closed
    FROM
      (
        SELECT
          DISTINCT
          plat_name    plat,
          trim(c_name) category
        FROM panda_competitor.crawler_category
        WHERE par_date = '$date'
      ) cate1
      LEFT JOIN
      (
        SELECT
          coalesce(cate1.plat_name, cate2.plat_name) plat,
          coalesce(cate1.c_name, cate2.c_name)       category,
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
              trim(c_name) c_name
            FROM panda_competitor.crawler_category
            WHERE par_date = '$date'
          ) cate1
          FULL JOIN
          (
            SELECT
              DISTINCT
              plat_name,
              trim(c_name) c_name
            FROM panda_competitor.crawler_category
            WHERE par_date = '$sub_1_days'
          ) cate2
            ON cate1.plat_name = cate2.plat_name AND cate1.c_name = cate2.c_name
      ) cate_all
        ON cate1.plat = cate_all.plat AND cate1.category = cate_all.category
  ) cate1
  LEFT JOIN
  (
    SELECT
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
              round(sum(anchors) / 60 * $minutes, 2) duration,
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
                WHERE par_date = '$date' AND task LIKE '%anchor' AND category != ''
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
              ELSE round(sum(anchors) / 60 * $minutes, 2) END duration,
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
                WHERE par_date = '$date' AND task LIKE '%detailanchor' AND category_sec != ''
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
          round(sum(anchors) / 60 * $minutes, 2) duration,
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
  ) t
    ON cate1.plat = t.plat AND cate1.category = t.category
  LEFT JOIN
  (
    SELECT
      dis1.plat,
      dis1.category,
      count(1)        lives,
      sum(CASE WHEN dis2.rid IS NULL
        THEN 1
          ELSE 0 END) new_anchors
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
            WHERE par_date = '$date' AND task LIKE '%anchor' AND category != ''
            UNION ALL
            SELECT
              DISTINCT
              split(task, 'detailanchor') [0] plat,
              category_sec,
              rid
            FROM panda_competitor.crawler_detail_anchor
            WHERE par_date = '$date' AND task LIKE '%detailanchor' AND category_sec != ''
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
    GROUP BY dis1.plat, dis1.category
  ) anchors
    ON cate1.plat = anchors.plat AND cate1.category = anchors.category;
    "