#!/bin/bash

date=$1
date=${date:=`date -d 'yesterday' +%Y%m%d`}
sub_6_days=`date -d "-6day $date" +%Y%m%d`
sub_7_days=`date -d "-7day $date" +%Y%m%d`

hive -e "
insert overwrite table panda_result.crawler_week_detail_anchor_ana partition(par_date)
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
  anc.live_days,
  anc.live_time,
  anc.live_time_per_day,
  anc.max_pcu,
  anc.cate_pcu_r,
  anc.plat_pcu_r,
  anc.pre_pcu_date,
  anc.pre_pcu,
  anc.suf_pcu_date,
  anc.suf_pcu,
  anc.suf_pcu-anc.pre_pcu  pcu_raise,
  row_number()
  OVER (PARTITION BY category_sec
    ORDER BY anc.suf_pcu-anc.pre_pcu DESC)           pcu_raise_r,
  anc.max_follower,
  anc.pre_followers_date,
  anc.pre_followers,
  anc.suf_followers_date,
  anc.suf_followers,
  anc.suf_followers-anc.pre_followers  followers_raise,
  row_number()
  OVER (PARTITION BY category_sec
    ORDER BY anc.suf_followers-anc.pre_followers DESC)     follower_raise_r,
  anc.rec_times,
  rank()
  OVER (PARTITION BY category_sec
    ORDER BY rec_times DESC)           rec_times_r,
  row_number()
  OVER (ORDER BY anc.suf_pcu - anc.pre_pcu DESC)             plat_pcu_raise_r,
  row_number()
  OVER (ORDER BY anc.suf_followers - anc.pre_followers DESC) plat_follower_raise_r,
  pre_weight_date,
  pre_weight,
  suf_weight_date,
  suf_weight,
  suf_weight - pre_weight,
  '$date'
FROM
  (
    SELECT
      r.rid,
      r.name,
      r.category_sec,
      r.weight_num,
      r.live_times,
      r.live_days,
      r.live_time,
      r.live_time_per_day,
      r.max_follower,
      r.max_pcu,
      r.cate_pcu_r,
      r.plat_pcu_r,
      CASE WHEN pre_pcu.par_date IS NULL
        THEN ''
      ELSE pre_pcu.par_date END            pre_pcu_date,
      CASE WHEN pre_pcu.max_pcu IS NULL
        THEN 0
      ELSE pre_pcu.max_pcu END             pre_pcu,
      CASE WHEN suf_pcu.par_date IS NULL
        THEN ''
      ELSE suf_pcu.par_date END            suf_pcu_date,
      CASE WHEN suf_pcu.max_pcu IS NULL
        THEN 0
      ELSE suf_pcu.max_pcu END             suf_pcu,
      CASE WHEN all_crawler.rid IS NULL
        THEN 1
      ELSE 0 END                           is_new,
      CASE WHEN rec_times.times > 0
        THEN rec_times.times
      ELSE 0 END                           rec_times,
      CASE WHEN pre_followers.par_date IS NULL
        THEN ''
      ELSE pre_followers.par_date END      pre_followers_date,
      CASE WHEN pre_followers.max_followers IS NULL
        THEN 0
      ELSE pre_followers.max_followers END pre_followers,
      CASE WHEN suf_followers.par_date IS NULL
        THEN ''
      ELSE suf_followers.par_date END      suf_followers_date,
      CASE WHEN suf_followers.max_followers IS NULL
        THEN 0
      ELSE suf_followers.max_followers END suf_followers,
      CASE WHEN pre_weight.par_date IS NULL
        THEN ''
      ELSE pre_weight.par_date END      pre_weight_date,
      CASE WHEN pre_weight.pre_weight IS NULL
        THEN 0
      ELSE pre_weight.pre_weight END pre_weight,
      CASE WHEN suf_weight.par_date IS NULL
        THEN ''
      ELSE suf_weight.par_date END      suf_weight_date,
      CASE WHEN suf_weight.suf_weight IS NULL
        THEN 0
      ELSE suf_weight.suf_weight END suf_weight
    FROM
      (
        SELECT
          rid,
          name,
          category_sec,
          weight_num,
          live_times,
          live_days,
          round(live_times / 4, 2)               live_time,
          round((live_times / 4) / live_days, 2) live_time_per_day,
          max_pcu,
          max_follower,
          row_number()
          OVER (PARTITION BY category_sec
            ORDER BY max_pcu DESC)               cate_pcu_r,
          row_number()
          OVER (
            ORDER BY max_pcu DESC)               plat_pcu_r
        FROM
          (
            SELECT
              rid,
              name,
              category_sec,
              max(max_pcu)             max_pcu,
              max(max_follower)        max_follower,
              max(weight_num)          weight_num,
              sum(live_times)          live_times,
              count(DISTINCT par_date) live_days
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
                  panda_competitor.crawler_detail_anchor
                WHERE par_date BETWEEN '$sub_6_days' AND '$date' AND task = 'douyudetailanchor' and category_sec !=''
                GROUP BY par_date, rid, name, category_sec
              ) anc
            GROUP BY rid, name, category_sec
          ) d_anc
      ) r
      LEFT JOIN
      (
        SELECT
          rid,
          name,
          category_sec,
          count(DISTINCT task_random) times
        FROM panda_competitor.crawler_indexrec_detail_anchor
        WHERE par_date BETWEEN '$sub_6_days' AND '$date' AND task = 'douyuindexrec'
        GROUP BY rid, name, category_sec
      ) rec_times
        ON r.rid = rec_times.rid AND r.name = rec_times.name AND
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
                ORDER BY par_date ASC) r
            FROM
              (
                SELECT
                  par_date,
                  rid,
                  name,
                  category_sec,
                  max(online_num) max_pcu
                FROM
                  panda_competitor.crawler_detail_anchor
                WHERE
                  par_date BETWEEN '$sub_6_days' AND '$date' AND task = 'douyudetailanchor' and category_sec !=''
                GROUP BY par_date, rid, name, category_sec
              ) d_anc
          ) r
        WHERE r.r = 1
      ) pre_pcu
        ON r.rid = pre_pcu.rid AND r.name = pre_pcu.name AND
           r.category_sec = pre_pcu.category_sec
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
                  panda_competitor.crawler_detail_anchor
                WHERE
                  par_date BETWEEN '$sub_6_days' AND '$date' AND task = 'douyudetailanchor' and category_sec !=''
                GROUP BY par_date, rid, name, category_sec
              ) d_anc
          ) r
        WHERE r.r = 1
      ) suf_pcu
        ON r.rid = suf_pcu.rid AND r.name = suf_pcu.name AND
           r.category_sec = suf_pcu.category_sec
        LEFT JOIN
      (
        SELECT
          par_date,
          rid,
          name,
          category_sec,
          pre_weight
        FROM
          (
            SELECT
              par_date,
              rid,
              name,
              category_sec,
              pre_weight,
              rank()
              OVER (PARTITION BY rid, name, category_sec
                ORDER BY par_date ASC) r
            FROM
              (
                SELECT
                  par_date,
                  rid,
                  name,
                  category_sec,
                  max(weight_num) pre_weight
                FROM
                  panda_competitor.crawler_detail_anchor
                WHERE
                  par_date BETWEEN '$sub_6_days' AND '$date' AND task = 'douyudetailanchor' and category_sec !=''
                GROUP BY par_date, rid, name, category_sec
              ) d_anc
          ) r
        WHERE r.r = 1
      ) pre_weight
        ON r.rid = pre_weight.rid AND r.name = pre_weight.name AND
           r.category_sec = pre_weight.category_sec
      LEFT JOIN
      (
        SELECT
          par_date,
          rid,
          name,
          category_sec,
          suf_weight
        FROM
          (
            SELECT
              par_date,
              rid,
              name,
              category_sec,
              suf_weight,
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
                  max(weight_num) suf_weight
                FROM
                  panda_competitor.crawler_detail_anchor
                WHERE
                  par_date BETWEEN '$sub_6_days' AND '$date' AND task = 'douyudetailanchor' and category_sec !=''
                GROUP BY par_date, rid, name, category_sec
              ) d_anc
          ) r
        WHERE r.r = 1
      ) suf_weight
        ON r.rid = suf_weight.rid AND r.name = suf_weight.name AND
           r.category_sec = suf_weight.category_sec
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
                ORDER BY par_date ASC) r
            FROM
              (
                SELECT
                  par_date,
                  rid,
                  name,
                  category_sec,
                  max(follower_num) max_followers
                FROM
                  panda_competitor.crawler_detail_anchor
                WHERE
                  par_date BETWEEN '$sub_6_days' AND '$date' AND task = 'douyudetailanchor' and category_sec !=''
                GROUP BY par_date, rid, name, category_sec
              ) d_anc
          ) r
        WHERE r.r = 1
      ) pre_followers
        ON r.rid = pre_followers.rid AND r.name = pre_followers.name AND
           r.category_sec = pre_followers.category_sec
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
                  panda_competitor.crawler_detail_anchor
                WHERE
                  par_date BETWEEN '$sub_6_days' AND '$date' AND task = 'douyudetailanchor' and category_sec !=''
                GROUP BY par_date, rid, name, category_sec
              ) d_anc
          ) r
        WHERE r.r = 1
      ) suf_followers
        ON r.rid = suf_followers.rid AND r.name = suf_followers.name AND
           r.category_sec = suf_followers.category_sec
      LEFT JOIN
      (
        SELECT rid
        FROM panda_competitor.crawler_distinct_anchor
        WHERE par_date = '$sub_7_days' AND plat = 'douyu'
      ) all_crawler
        ON r.rid = all_crawler.rid
  ) anc
UNION ALL
SELECT
  anc.rid,
  anc.name,
  'huya',
  anc.category_sec,
  CASE WHEN anc.weight_num > 1000
    THEN concat(round(anc.weight_num / 1000, 2), 'kg')
  WHEN anc.weight_num > 1000000
    THEN concat(round(anc.weight_num / 1000000, 2), 't')
  ELSE concat(anc.weight_num, 'g') END weight_str,
  anc.weight_num,
  anc.is_new,
  anc.live_times,
  anc.live_days,
  anc.live_time,
  anc.live_time_per_day,
  anc.max_pcu,
  anc.cate_pcu_r,
  anc.plat_pcu_r,
  anc.pre_pcu_date,
  anc.pre_pcu,
  anc.suf_pcu_date,
  anc.suf_pcu,
  anc.suf_pcu-anc.pre_pcu  pcu_raise,
  row_number()
  OVER (PARTITION BY category_sec
    ORDER BY anc.suf_pcu-anc.pre_pcu DESC)           pcu_raise_r,
  anc.max_follower,
  anc.pre_followers_date,
  anc.pre_followers,
  anc.suf_followers_date,
  anc.suf_followers,
  anc.suf_followers-anc.pre_followers  followers_raise,
  row_number()
  OVER (PARTITION BY category_sec
    ORDER BY anc.suf_followers-anc.pre_followers DESC)     follower_raise_r,
  anc.rec_times,
  rank()
  OVER (PARTITION BY category_sec
    ORDER BY rec_times DESC)           rec_times_r,
  row_number()
  OVER (ORDER BY anc.suf_pcu - anc.pre_pcu DESC)             plat_pcu_raise_r,
  row_number()
  OVER (ORDER BY anc.suf_followers - anc.pre_followers DESC) plat_follower_raise_r,
  pre_weight_date,
  pre_weight,
  suf_weight_date,
  suf_weight,
  suf_weight - pre_weight,
  '$date'
FROM
  (
    SELECT
      r.rid,
      r.name,
      r.category_sec,
      r.weight_num,
      r.live_times,
      r.live_days,
      r.live_time,
      r.live_time_per_day,
      r.max_follower,
      r.max_pcu,
      r.cate_pcu_r,
      r.plat_pcu_r,
      CASE WHEN pre_pcu.par_date IS NULL
        THEN ''
      ELSE pre_pcu.par_date END            pre_pcu_date,
      CASE WHEN pre_pcu.max_pcu IS NULL
        THEN 0
      ELSE pre_pcu.max_pcu END             pre_pcu,
      CASE WHEN suf_pcu.par_date IS NULL
        THEN ''
      ELSE suf_pcu.par_date END            suf_pcu_date,
      CASE WHEN suf_pcu.max_pcu IS NULL
        THEN 0
      ELSE suf_pcu.max_pcu END             suf_pcu,
      CASE WHEN all_crawler.rid IS NULL
        THEN 1
      ELSE 0 END                           is_new,
      CASE WHEN rec_times.times > 0
        THEN rec_times.times
      ELSE 0 END                           rec_times,
      CASE WHEN pre_followers.par_date IS NULL
        THEN ''
      ELSE pre_followers.par_date END      pre_followers_date,
      CASE WHEN pre_followers.max_followers IS NULL
        THEN 0
      ELSE pre_followers.max_followers END pre_followers,
      CASE WHEN suf_followers.par_date IS NULL
        THEN ''
      ELSE suf_followers.par_date END      suf_followers_date,
      CASE WHEN suf_followers.max_followers IS NULL
        THEN 0
      ELSE suf_followers.max_followers END suf_followers,
      CASE WHEN pre_weight.par_date IS NULL
        THEN ''
      ELSE pre_weight.par_date END      pre_weight_date,
      CASE WHEN pre_weight.pre_weight IS NULL
        THEN 0
      ELSE pre_weight.pre_weight END pre_weight,
      CASE WHEN suf_weight.par_date IS NULL
        THEN ''
      ELSE suf_weight.par_date END      suf_weight_date,
      CASE WHEN suf_weight.suf_weight IS NULL
        THEN 0
      ELSE suf_weight.suf_weight END suf_weight
    FROM
      (
        SELECT
          rid,
          name,
          category_sec,
          weight_num,
          live_times,
          live_days,
          round(live_times / 4, 2)               live_time,
          round((live_times / 4) / live_days, 2) live_time_per_day,
          max_pcu,
          max_follower,
          row_number()
          OVER (PARTITION BY category_sec
            ORDER BY max_pcu DESC)               cate_pcu_r,
          row_number()
          OVER (
            ORDER BY max_pcu DESC)               plat_pcu_r
        FROM
          (
            SELECT
              rid,
              name,
              category_sec,
              max(max_pcu)             max_pcu,
              max(max_follower)        max_follower,
              max(weight_num)          weight_num,
              sum(live_times)          live_times,
              count(DISTINCT par_date) live_days
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
                  panda_competitor.crawler_detail_anchor
                WHERE par_date BETWEEN '$sub_6_days' AND '$date' AND task = 'huyadetailanchor' and category_sec !=''
                GROUP BY par_date, rid, name, category_sec
              ) anc
            GROUP BY rid, name, category_sec
          ) d_anc
      ) r
      LEFT JOIN
      (
        SELECT
          rid,
          name,
          category_sec,
          count(DISTINCT task_random) times
        FROM panda_competitor.crawler_indexrec_detail_anchor
        WHERE par_date BETWEEN '$sub_6_days' AND '$date' AND task = 'huyaindexrec'
        GROUP BY rid, name, category_sec
      ) rec_times
        ON r.rid = rec_times.rid AND r.name = rec_times.name AND
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
                ORDER BY par_date ASC) r
            FROM
              (
                SELECT
                  par_date,
                  rid,
                  name,
                  category_sec,
                  max(online_num) max_pcu
                FROM
                  panda_competitor.crawler_detail_anchor
                WHERE
                  par_date BETWEEN '$sub_6_days' AND '$date' AND task = 'huyadetailanchor' and category_sec !=''
                GROUP BY par_date, rid, name, category_sec
              ) d_anc
          ) r
        WHERE r.r = 1
      ) pre_pcu
        ON r.rid = pre_pcu.rid AND r.name = pre_pcu.name AND
           r.category_sec = pre_pcu.category_sec
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
                  panda_competitor.crawler_detail_anchor
                WHERE
                  par_date BETWEEN '$sub_6_days' AND '$date' AND task = 'huyadetailanchor' and category_sec !=''
                GROUP BY par_date, rid, name, category_sec
              ) d_anc
          ) r
        WHERE r.r = 1
      ) suf_pcu
        ON r.rid = suf_pcu.rid AND r.name = suf_pcu.name AND
           r.category_sec = suf_pcu.category_sec
        LEFT JOIN
      (
        SELECT
          par_date,
          rid,
          name,
          category_sec,
          pre_weight
        FROM
          (
            SELECT
              par_date,
              rid,
              name,
              category_sec,
              pre_weight,
              rank()
              OVER (PARTITION BY rid, name, category_sec
                ORDER BY par_date ASC) r
            FROM
              (
                SELECT
                  par_date,
                  rid,
                  name,
                  category_sec,
                  max(weight_num) pre_weight
                FROM
                  panda_competitor.crawler_detail_anchor
                WHERE
                  par_date BETWEEN '$sub_6_days' AND '$date' AND task = 'huyadetailanchor' and category_sec !=''
                GROUP BY par_date, rid, name, category_sec
              ) d_anc
          ) r
        WHERE r.r = 1
      ) pre_weight
        ON r.rid = pre_weight.rid AND r.name = pre_weight.name AND
           r.category_sec = pre_weight.category_sec
      LEFT JOIN
      (
        SELECT
          par_date,
          rid,
          name,
          category_sec,
          suf_weight
        FROM
          (
            SELECT
              par_date,
              rid,
              name,
              category_sec,
              suf_weight,
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
                  max(weight_num) suf_weight
                FROM
                  panda_competitor.crawler_detail_anchor
                WHERE
                  par_date BETWEEN '$sub_6_days' AND '$date' AND task = 'huyadetailanchor' and category_sec !=''
                GROUP BY par_date, rid, name, category_sec
              ) d_anc
          ) r
        WHERE r.r = 1
      ) suf_weight
        ON r.rid = suf_weight.rid AND r.name = suf_weight.name AND
           r.category_sec = suf_weight.category_sec
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
                ORDER BY par_date ASC) r
            FROM
              (
                SELECT
                  par_date,
                  rid,
                  name,
                  category_sec,
                  max(follower_num) max_followers
                FROM
                  panda_competitor.crawler_detail_anchor
                WHERE
                  par_date BETWEEN '$sub_6_days' AND '$date' AND task = 'huyadetailanchor' and category_sec !=''
                GROUP BY par_date, rid, name, category_sec
              ) d_anc
          ) r
        WHERE r.r = 1
      ) pre_followers
        ON r.rid = pre_followers.rid AND r.name = pre_followers.name AND
           r.category_sec = pre_followers.category_sec
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
                  panda_competitor.crawler_detail_anchor
                WHERE
                  par_date BETWEEN '$sub_6_days' AND '$date' AND task = 'huyadetailanchor' and category_sec !=''
                GROUP BY par_date, rid, name, category_sec
              ) d_anc
          ) r
        WHERE r.r = 1
      ) suf_followers
        ON r.rid = suf_followers.rid AND r.name = suf_followers.name AND
           r.category_sec = suf_followers.category_sec
      LEFT JOIN
      (
        SELECT rid
        FROM panda_competitor.crawler_distinct_anchor
        WHERE par_date = '$sub_7_days' AND plat = 'huya'
      ) all_crawler
        ON r.rid = all_crawler.rid
  ) anc;
"
