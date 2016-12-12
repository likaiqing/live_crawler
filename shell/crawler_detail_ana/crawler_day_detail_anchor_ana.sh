#!/bin/bash

date=$1
date=${date:=`date -d 'yesterday' +%Y%m%d`}
sub_1_days=`date -d "-1day $date" +%Y%m%d`
sub_7_days=`date -d "-7day $date" +%Y%m%d`

hive -e "
insert overwrite table panda_result.crawler_day_detail_anchor_ana partition(par_date)
SELECT
  anc.rid,
  anc.name,
  'douyu',
  anc.category_sec,
  CASE WHEN anc.weight_num > 1000
    THEN concat(round(anc.weight_num / 1000, 2), 'kg')
  WHEN anc.weight_num > 1000000
    THEN concat(round(anc.weight_num / 1000000, 2), 't')
  ELSE concat(anc.weight_num, 'g') END weight_str,
  anc.weight_num,
  anc.is_new,
  anc.live_times,
  anc.live_times                       live_time,
  anc.max_pcu,
  anc.cate_pcu_r,
  anc.plat_pcu_r,
  anc.last_max_pcu,
  anc.pcu_raise,
  row_number()
  OVER (PARTITION BY par_date, category_sec
    ORDER BY pcu_raise DESC)           pcu_raise_r,
  anc.max_follower,
  anc.last_max_followers_date,
  anc.last_max_followers,
  anc.followers_raise,
  row_number()
  OVER (PARTITION BY par_date, category_sec
    ORDER BY followers_raise DESC)     follower_raise_r,
  anc.rec_times,
  rank()
  OVER (PARTITION BY par_date, category_sec
    ORDER BY rec_times DESC)           rec_times_r,
  row_number()
  OVER (PARTITION BY par_date
    ORDER BY pcu_raise DESC)           plat_pcu_raise_r,
  row_number()
  OVER (PARTITION BY par_date
    ORDER BY followers_raise DESC)     plat_follower_raise_r,
  last_max_weight,
  weight_raise,
  anc.par_date
FROM
  (
    SELECT
      r.par_date,
      r.rid,
      r.name,
      r.category_sec,
      r.weight_num,
      r.live_times,
      r.max_follower,
      r.max_pcu,
      r.cate_pcu_r,
      r.plat_pcu_r,
      CASE WHEN last_pcu.max_pcu IS NULL
        THEN 0
      ELSE last_pcu.max_pcu END last_max_pcu,
      CASE WHEN last_pcu.max_pcu IS NULL
        THEN r.max_pcu
      ELSE r.max_pcu - last_pcu.max_pcu END                     pcu_raise,
      CASE WHEN all_crawler.rid IS NULL
        THEN 1
      ELSE 0 END                                                is_new,
      CASE WHEN rec_times.times > 0
        THEN rec_times.times
      ELSE 0 END                                                rec_times,
      CASE WHEN last_followers.max_followers IS NULL
        THEN 0
      ELSE last_followers.par_date END                          last_max_followers_date,
      CASE WHEN last_followers.max_followers IS NULL
        THEN 0
      ELSE last_followers.max_followers END                     last_max_followers,
      CASE WHEN last_followers.max_followers IS NULL
        THEN r.max_follower
      ELSE r.max_follower - last_followers.max_followers END    followers_raise,
      CASE WHEN last_weight.par_date IS NULL
        THEN ''
      ELSE last_weight.max_weight END last_max_weight,
      CASE WHEN last_weight.par_date IS NULL
        THEN r.weight_num
      ELSE r.weight_num - last_weight.max_weight END                     weight_raise
    FROM
      (
        SELECT
          par_date,
          rid,
          name,
          category_sec,
          weight_num,
          live_times,
          max_pcu,
          max_follower,
          row_number()
          OVER (PARTITION BY par_date, category_sec
            ORDER BY max_pcu DESC) cate_pcu_r,
          row_number()
          OVER (PARTITION BY par_date
            ORDER BY max_pcu DESC) plat_pcu_r
        FROM
          (
            SELECT
              par_date,
              rid,
              name,
              category_sec,
              max(online_num)             max_pcu,
              max(follower_num)           max_follower,
              max(weight_num)             weight_num,
              count(DISTINCT task_random) live_times
            FROM
              panda_result.panda_detail_anchor_crawler
            WHERE par_date = '$date' AND task = 'douyudetailanchor'
            GROUP BY par_date, rid, name, category_sec
          ) d_anc
      ) r
      LEFT JOIN
      (
        SELECT
          par_date,
          rid,
          name,
          category_sec,
          count(DISTINCT task_random) times
        FROM panda_result.panda_detail_anchor_crawler
        WHERE par_date = '$date' AND task = 'douyuindexrec'
        GROUP BY par_date, rid, name, category_sec
      ) rec_times
        ON r.par_date = rec_times.par_date AND r.rid = rec_times.rid AND r.name = rec_times.name AND
           r.category_sec = rec_times.category_sec
      LEFT JOIN
      (
        SELECT
          par_date,
          rid,
          name,
          category_sec,
          max_pcu
        FROM
          (
            SELECT
              par_date,
              rid,
              name,
              category_sec,
              max_pcu,
              rank()
              OVER (PARTITION BY rid, name, category_sec
                ORDER BY par_date DESC) r
            FROM
              (
                SELECT
                  par_date,
                  rid,
                  name,
                  category_sec,
                  max(online_num) max_pcu
                FROM
                  panda_result.panda_detail_anchor_crawler
                WHERE
                  par_date BETWEEN '$sub_7_days' AND '$sub_1_days' AND task = 'douyudetailanchor'
                GROUP BY par_date, rid, name, category_sec
              ) d_anc
          ) r
        WHERE r.r = 1
      ) last_pcu
        ON r.rid = last_pcu.rid AND r.name = last_pcu.name AND
           r.category_sec = last_pcu.category_sec
      LEFT JOIN
      (
        SELECT
          par_date,
          rid,
          name,
          category_sec,
          max_weight
        FROM
          (
            SELECT
              par_date,
              rid,
              name,
              category_sec,
              max_weight,
              rank()
              OVER (PARTITION BY rid, name, category_sec
                ORDER BY par_date DESC) r
            FROM
              (
                SELECT
                  par_date,
                  rid,
                  name,
                  category_sec,
                  max(weight_num) max_weight
                FROM
                  panda_result.panda_detail_anchor_crawler
                WHERE
                  par_date BETWEEN '$sub_7_days' AND '$sub_1_days' AND task = 'douyudetailanchor'
                GROUP BY par_date, rid, name, category_sec
              ) d_anc
          ) r
        WHERE r.r = 1
      ) last_weight
        ON r.rid = last_weight.rid AND r.name = last_weight.name AND
           r.category_sec = last_weight.category_sec
      LEFT JOIN
      (
        SELECT
          par_date,
          rid,
          name,
          category_sec,
          max_followers
        FROM
          (
            SELECT
              par_date,
              rid,
              name,
              category_sec,
              max_followers,
              rank()
              OVER (PARTITION BY rid, name, category_sec
                ORDER BY par_date DESC) r
            FROM
              (
                SELECT
                  par_date,
                  rid,
                  name,
                  category_sec,
                  max(follower_num) max_followers
                FROM
                  panda_result.panda_detail_anchor_crawler
                WHERE
                  par_date BETWEEN '$sub_7_days' AND '$sub_1_days' AND task = 'douyudetailanchor'
                GROUP BY par_date, rid, name, category_sec
              ) d_anc
          ) r
        WHERE r.r = 1
      ) last_followers
        ON r.rid = last_followers.rid AND r.name = last_followers.name AND
           r.category_sec = last_followers.category_sec
      LEFT JOIN
      (
        SELECT
          rid
        FROM panda_result.panda_distinct_detail_anchor_crawler
        WHERE par_date = '$sub_1_days' AND task = 'douyudetailanchor'
      ) all_crawler
        ON r.rid = all_crawler.rid
  ) anc
UNION ALL
SELECT
  anc.rid,
  anc.name,
  'huya',
  anc.category_sec,
  '',
  anc.weight_num,
  anc.is_new,
  anc.live_times,
  round(anc.live_times / 4, 2)     live_time,
  anc.max_pcu,
  anc.cate_pcu_r,
  anc.plat_pcu_r,
  anc.last_max_pcu,
  anc.pcu_raise,
  row_number()
  OVER (PARTITION BY par_date, category_sec
    ORDER BY pcu_raise DESC)       pcu_raise_r,
  anc.max_follower,
  anc.last_max_followers_date,
  anc.last_max_followers,
  anc.followers_raise,
  row_number()
  OVER (PARTITION BY par_date, category_sec
    ORDER BY followers_raise DESC) follower_raise_r,
  anc.rec_times,
  rank()
  OVER (PARTITION BY par_date, category_sec
    ORDER BY rec_times DESC)       rec_times_r,
  row_number()
  OVER (PARTITION BY par_date
    ORDER BY pcu_raise DESC)           plat_pcu_raise_r,
  row_number()
  OVER (PARTITION BY par_date
    ORDER BY followers_raise DESC)     plat_follower_raise_r,
  last_max_weight,
  weight_raise,
  anc.par_date
FROM
  (
    SELECT
      r.par_date,
      r.rid,
      r.name,
      r.category_sec,
      r.weight_num,
      r.live_times,
      r.max_follower,
      r.max_pcu,
      r.cate_pcu_r,
      r.plat_pcu_r,
      CASE WHEN last_pcu.max_pcu IS NULL
        THEN 0
      ELSE last_pcu.max_pcu END last_max_pcu,
      CASE WHEN last_pcu.max_pcu IS NULL
        THEN r.max_pcu
      ELSE r.max_pcu - last_pcu.max_pcu END                     pcu_raise,
      CASE WHEN all_crawler.rid IS NULL
        THEN 1
      ELSE 0 END                                                is_new,
      CASE WHEN rec_times.times > 0
        THEN rec_times.times
      ELSE 0 END                                                rec_times,
      CASE WHEN last_followers.max_followers IS NULL
        THEN 0
      ELSE last_followers.par_date END                          last_max_followers_date,
      CASE WHEN last_followers.max_followers IS NULL
        THEN 0
      ELSE last_followers.max_followers END                     last_max_followers,
      CASE WHEN last_followers.max_followers IS NULL
        THEN r.max_follower
      ELSE r.max_follower - last_followers.max_followers END    followers_raise,
      CASE WHEN last_weight.par_date IS NULL
        THEN ''
      ELSE last_weight.max_weight END last_max_weight,
      CASE WHEN last_weight.par_date IS NULL
        THEN r.weight_num
      ELSE r.weight_num - last_weight.max_weight END                     weight_raise
    FROM
      (
        SELECT
          par_date,
          rid,
          name,
          category_sec,
          weight_num,
          live_times,
          max_pcu,
          max_follower,
          row_number()
          OVER (PARTITION BY par_date, category_sec
            ORDER BY max_pcu DESC) cate_pcu_r,
          row_number()
          OVER (PARTITION BY par_date
            ORDER BY max_pcu DESC) plat_pcu_r
        FROM
          (
            SELECT
              par_date,
              rid,
              name,
              category_sec,
              max(online_num)             max_pcu,
              max(follower_num)           max_follower,
              max(weight_num)             weight_num,
              count(DISTINCT task_random) live_times
            FROM
              panda_result.panda_detail_anchor_crawler
            WHERE par_date = '$date' AND task = 'huyadetailanchor'
            GROUP BY par_date, rid, name, category_sec
          ) d_anc
      ) r
      LEFT JOIN
      (
        SELECT
          par_date,
          rid,
          name,
          category_sec,
          count(DISTINCT task_random) times
        FROM panda_result.panda_detail_anchor_crawler
        WHERE par_date = '$date' AND task = 'huyaindexrec'
        GROUP BY par_date, rid, name, category_sec
      ) rec_times
        ON r.par_date = rec_times.par_date AND r.rid = rec_times.rid AND r.name = rec_times.name AND
           r.category_sec = rec_times.category_sec
      LEFT JOIN
      (
        SELECT
          par_date,
          rid,
          name,
          category_sec,
          max_pcu
        FROM
          (
            SELECT
              par_date,
              rid,
              name,
              category_sec,
              max_pcu,
              rank()
              OVER (PARTITION BY rid, name, category_sec
                ORDER BY par_date DESC) r
            FROM
              (
                SELECT
                  par_date,
                  rid,
                  name,
                  category_sec,
                  max(online_num) max_pcu
                FROM
                  panda_result.panda_detail_anchor_crawler
                WHERE
                  par_date BETWEEN '$sub_7_days' AND '$sub_1_days' AND task = 'huyadetailanchor'
                GROUP BY par_date, rid, name, category_sec
              ) d_anc
          ) r
        WHERE r.r = 1
      ) last_pcu
        ON r.rid = last_pcu.rid AND r.name = last_pcu.name AND
           r.category_sec = last_pcu.category_sec
        LEFT JOIN
      (
        SELECT
          par_date,
          rid,
          name,
          category_sec,
          max_weight
        FROM
          (
            SELECT
              par_date,
              rid,
              name,
              category_sec,
              max_weight,
              rank()
              OVER (PARTITION BY rid, name, category_sec
                ORDER BY par_date DESC) r
            FROM
              (
                SELECT
                  par_date,
                  rid,
                  name,
                  category_sec,
                  max(weight_num) max_weight
                FROM
                  panda_result.panda_detail_anchor_crawler
                WHERE
                  par_date BETWEEN '$sub_7_days' AND '$sub_1_days' AND task = 'huyadetailanchor'
                GROUP BY par_date, rid, name, category_sec
              ) d_anc
          ) r
        WHERE r.r = 1
      ) last_weight
        ON r.rid = last_weight.rid AND r.name = last_weight.name AND
           r.category_sec = last_weight.category_sec
      LEFT JOIN
      (
        SELECT
          par_date,
          rid,
          name,
          category_sec,
          max_followers
        FROM
          (
            SELECT
              par_date,
              rid,
              name,
              category_sec,
              max_followers,
              rank()
              OVER (PARTITION BY rid, name, category_sec
                ORDER BY par_date DESC) r
            FROM
              (
                SELECT
                  par_date,
                  rid,
                  name,
                  category_sec,
                  max(follower_num) max_followers
                FROM
                  panda_result.panda_detail_anchor_crawler
                WHERE
                  par_date BETWEEN '$sub_7_days' AND '$sub_1_days' AND task = 'huyadetailanchor'
                GROUP BY par_date, rid, name, category_sec
              ) d_anc
          ) r
        WHERE r.r = 1
      ) last_followers
        ON r.rid = last_followers.rid AND r.name = last_followers.name AND
           r.category_sec = last_followers.category_sec
      LEFT JOIN
      (
        SELECT
          rid
        FROM panda_result.panda_distinct_detail_anchor_crawler
        WHERE par_date = '$sub_1_days' AND task = 'huyadetailanchor'
      ) all_crawler
        ON  r.rid = all_crawler.rid
  ) anc;
"
