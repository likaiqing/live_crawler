#!/bin/bash

#按版区统计主播的PCU排名取前10个
hive -e "
set hive.cli.print.header=true;
SELECT
  par_date,
  r.rid,            --主播id
  r.plat,           --平台
  category,         --版区
  populary_num,     --PCU
  r,                --排名
  dis.name          --主播昵称
FROM
  (
    SELECT
      par_date,
      rid,
      plat,
      category,
      populary_num,
      r
    FROM
      (
        SELECT
          par_date,
          rid,
          plat,
          category,
          populary_num,
          row_number()
          OVER (PARTITION BY par_date, plat, category
            ORDER BY populary_num DESC) r
        FROM
          (
            SELECT
              par_date,
              rid,
              plat,
              category,
              max(populary_num) populary_num
            FROM
              panda_competitor.crawler_anchor
            WHERE par_date BETWEEN '20161201' AND '20170120' AND task LIKE '%anchor' AND CATEGORY != ''
            GROUP BY par_date, rid, plat, category
          ) pop
      ) r
    WHERE r.r <= 10) r
  JOIN
  (
    SELECT DISTINCT
      plat_name,
      trim(c_name) c_name
    FROM panda_competitor.crawler_category
    WHERE par_date BETWEEN '20161201' AND '20170120'
  ) cate
    ON r.plat = cate.plat_name AND r.category = cate.c_name
  JOIN
  (
    SELECT
      rid,
      plat,
      name
    FROM panda_competitor.crawler_distinct_anchor
    WHERE par_date = '20170123' and rid !='0'
  ) dis
    ON r.rid = dis.rid AND r.plat = dis.plat;
" > ~/tmp/anchor_pcu_rank_by_category.csv
#按版区统计主播的订阅排名取前10个
hive -e "
set hive.cli.print.header=true;
SELECT
  par_date,
  r.rid,
  r.plat,
  category_sec,     --版区
  follower_num,     --订阅数
  r,
  dis.name
FROM
  (
    SELECT
      par_date,
      rid,
      plat,
      category_sec,
      follower_num,
      r
    FROM
      (
        SELECT
          par_date,
          rid,
          plat,
          category_sec,
          follower_num,
          row_number()
          OVER (PARTITION BY par_date, plat, category_sec
            ORDER BY follower_num DESC) r
        FROM
          (
            SELECT
              par_date,
              rid,
              split(task, 'detailanchor') [0] plat,
              category_sec,
              max(follower_num)               follower_num
            FROM panda_competitor.crawler_detail_anchor
            WHERE par_date BETWEEN '20161201' AND '20170120' AND task LIKE '%detailanchor' AND category_sec != ''
            GROUP BY par_date, rid, split(task, 'detailanchor') [0], category_sec
          ) fol
      ) r
    WHERE r.r <= 10
  ) r
  JOIN
  (
    SELECT DISTINCT
      plat_name,
      trim(c_name) c_name
    FROM panda_competitor.crawler_category
    WHERE par_date BETWEEN '20161201' AND '20170120'
  ) cate
    ON r.plat = cate.plat_name AND r.category_sec = cate.c_name
    JOIN
  (
    SELECT
      rid,
      plat,
      name
    FROM panda_competitor.crawler_distinct_anchor
    WHERE par_date = '20170123' and rid !='0'
  ) dis
    ON r.rid = dis.rid AND r.plat = dis.plat;
" > ~/tmp/anchor_follow_rank_by_category.csv
#按版区统计主播的体重排名取前10个
hive -e "
set hive.cli.print.header=true;
SELECT
  par_date,
  r.rid,
  r.plat,
  category_sec,
  weight_num,       --体重
  r,
  dis.name
FROM
  (
    SELECT
      par_date,
      rid,
      plat,
      category_sec,
      weight_num,
      r
    FROM
      (
        SELECT
          par_date,
          rid,
          plat,
          category_sec,
          weight_num,
          row_number()
          OVER (PARTITION BY par_date, plat, category_sec
            ORDER BY weight_num DESC) r
        FROM
          (
            SELECT
              par_date,
              rid,
              split(task, 'detailanchor') [0] plat,
              category_sec,
              max(weight_num)               weight_num
            FROM panda_competitor.crawler_detail_anchor
            WHERE par_date BETWEEN '20161201' AND '20170120' AND task LIKE '%detailanchor' AND category_sec != ''
            GROUP BY par_date, rid, split(task, 'detailanchor') [0], category_sec
          ) fol
      ) r
    WHERE r.r <= 10
  ) r
  JOIN
  (
    SELECT DISTINCT
      plat_name,
      trim(c_name) c_name
    FROM panda_competitor.crawler_category
    WHERE par_date BETWEEN '20161201' AND '20170120'
  ) cate
    ON r.plat = cate.plat_name AND r.category_sec = cate.c_name
    JOIN
  (
    SELECT
      rid,
      plat,
      name
    FROM panda_competitor.crawler_distinct_anchor
    WHERE par_date = '20170123' and rid !='0'
  ) dis
    ON r.rid = dis.rid AND r.plat = dis.plat;
" > ~/tmp/anchor_weight_rank_by_category.csv
#按平台统计主播的PCU排名取前10个
hive -e "
set hive.cli.print.header=true;
SELECT
  par_date,
  r.rid,
  r.plat,
  r.category,
  r.populary_num,   --PCU
  r.r,
  dis.name
FROM
  (
    SELECT
      par_date,
      rid,
      plat,
      category,
      populary_num,
      r
    FROM
      (
        SELECT
          par_date,
          rid,
          plat,
          category,
          populary_num,
          row_number()
          OVER (PARTITION BY par_date, plat
            ORDER BY populary_num DESC) r
        FROM
          (
            SELECT
              par_date,
              rid,
              plat,
              category,
              max(populary_num) populary_num
            FROM
              panda_competitor.crawler_anchor
            WHERE par_date BETWEEN '20161201' AND '20170120' AND task LIKE '%anchor' AND CATEGORY != ''
            GROUP BY par_date, rid, plat, category
          ) tmp
      ) r
    WHERE r.r <= 10
  ) r
  JOIN
  (
    SELECT
      rid,
      plat,
      name
    FROM panda_competitor.crawler_distinct_anchor
    WHERE par_date = '20170123' and rid !='0'
  ) dis
    ON r.rid = dis.rid AND r.plat = dis.plat;
" > ~/tmp/anchor_pcu_rank_by_plat.csv
#按平台统计主播的订阅排名取前10个
hive -e "
set hive.cli.print.header=true;
SELECT
  par_date,
  r.rid,
  r.plat,
  r.category_sec,
  r.follower_num,
  r.r,
  dis.name
FROM
  (
    SELECT
      par_date,
      rid,
      plat,
      category_sec,
      follower_num,
      r
    FROM
      (
        SELECT
          par_date,
          rid,
          plat,
          category_sec,
          follower_num,
          row_number()
          OVER (PARTITION BY par_date, plat
            ORDER BY follower_num DESC) r
        FROM
          (
            SELECT
              par_date,
              rid,
              split(task, 'detailanchor') [0] plat,
              category_sec,
              max(follower_num)               follower_num
            FROM panda_competitor.crawler_detail_anchor
            WHERE par_date BETWEEN '20161201' AND '20170120' AND task LIKE '%detailanchor' AND category_sec != ''
            GROUP BY par_date, rid, split(task, 'detailanchor') [0], category_sec
          ) fol
      ) r
    WHERE r.r <= 10
  ) r
  JOIN
  (
    SELECT
      rid,
      plat,
      name
    FROM panda_competitor.crawler_distinct_anchor
    WHERE par_date = '20170123' and rid !='0'
  ) dis
    ON r.rid = dis.rid AND r.plat = dis.plat;
" > ~/tmp/anchor_follow_rank_by_plat.csv

#按平台统计主播的体重排名取前10个
hive -e "
set hive.cli.print.header=true;
SELECT
  r.par_date,
  r.rid,
  r.plat,
  r.category_sec,
  r.weight_num,
  r.r,
  dis.name
FROM
  (
    SELECT
      par_date,
      rid,
      plat,
      category_sec,
      weight_num,
      r
    FROM
      (
        SELECT
          par_date,
          rid,
          plat,
          category_sec,
          weight_num,
          row_number()
          OVER (PARTITION BY par_date, plat
            ORDER BY weight_num DESC) r
        FROM
          (
            SELECT
              par_date,
              rid,
              split(task, 'detailanchor') [0] plat,
              category_sec,
              max(weight_num)                 weight_num
            FROM panda_competitor.crawler_detail_anchor
            WHERE par_date BETWEEN '20161201' AND '20170120' AND task LIKE '%detailanchor' AND category_sec != ''
            GROUP BY par_date, rid, split(task, 'detailanchor') [0], category_sec
          ) fol
      ) r
    WHERE r.r <= 10
  ) r
  JOIN
  (
    SELECT
      rid,
      plat,
      name
    FROM panda_competitor.crawler_distinct_anchor
    WHERE par_date = '20170123' and rid !='0'
  ) dis
    ON r.rid = dis.rid AND r.plat = dis.plat;
" > ~/tmp/anchor_weight_rank_by_plat.csv
#按版区统计被推荐的次数和房间数
hive -e "
set hive.cli.print.header=true;
SELECT
  par_date,
  plat,
  category_sec,
  recs,             --抓取到推荐次数
  rec_rooms         --抓取到推荐主播数
FROM
  (
    SELECT
      par_date,
      split(task, 'index') [0] plat,
      category_sec,
      count(1)                 recs,
      count(DISTINCT rid)      rec_rooms
    FROM
      panda_competitor.crawler_indexrec_detail_anchor
    WHERE par_date BETWEEN '20161201' AND '20170120' AND task LIKE '%indexrec'
    GROUP BY par_date, split(task, 'index') [0], category_sec
  ) rec
  JOIN
  (
    SELECT DISTINCT
      plat_name,
      trim(c_name) c_name
    FROM panda_competitor.crawler_category
    WHERE par_date BETWEEN '20161201' AND '20170120'
  ) cate
    ON rec.plat = cate.plat_name AND rec.category_sec = cate.c_name;
" > ~/tmp/rec_rooms_by_category.csv
#推荐房间的订阅体重变化
hive -e "
set hive.cli.print.header=true;
SELECT
  r.par_date,
  r.rid,
  r.plat,
  r.category_sec,
  r.follower_change,        --订阅数变化
  r.weight_change,          --体重数变化
  dis.name
FROM
  (
    SELECT
      fir.par_date,
      fir.rid,
      fir.plat,
      fir.category_sec,
      coalesce(last.follower_num, 0) - coalesce(fir.follower_num, 0) follower_change,
      coalesce(last.weight_num, 0) - coalesce(fir.weight_num, 0)     weight_change
    FROM
      (
        SELECT
          par_date,
          rid,
          plat,
          category_sec,
          follower_num,
          weight_num
        FROM
          (
            SELECT
              par_date,
              rid,
              split(task, 'index') [0]    plat,
              category_sec,
              follower_num,
              weight_num,
              row_number()
              OVER (PARTITION BY par_date, rid, split(task, 'index') [0], category_sec
                ORDER BY create_time ASC) r
            FROM panda_competitor.crawler_indexrec_detail_anchor
            WHERE par_date BETWEEN '20161201' AND '20170120'
          ) fir
        WHERE fir.r = 1
      ) fir
      LEFT JOIN
      (
        SELECT
          par_date,
          rid,
          plat,
          category_sec,
          follower_num,
          weight_num
        FROM
          (
            SELECT
              par_date,
              rid,
              split(task, 'index') [0]     plat,
              category_sec,
              follower_num,
              weight_num,
              row_number()
              OVER (PARTITION BY par_date, rid, split(task, 'index') [0], category_sec
                ORDER BY create_time DESC) r
            FROM panda_competitor.crawler_indexrec_detail_anchor
            WHERE par_date BETWEEN '20161201' AND '20170120'
          ) last
        WHERE last.r = 1
      ) last
        ON fir.par_date = last.par_date AND fir.rid = last.rid AND fir.plat = last.plat AND
           fir.category_sec = last.category_sec
  ) r
  JOIN
  (
    SELECT
      rid,
      plat,
      name
    FROM panda_competitor.crawler_distinct_anchor
    WHERE par_date = '20170123' and rid !='0'
  ) dis
    ON r.rid = dis.rid AND r.plat = dis.plat;
" > ~/tmp/rec_fol_weigh_change.csv
#版区的PCU订阅统计
hive -e "
set hive.cli.print.header=true;
SELECT
  par_date,
  plat,
  category,
  pcu,
  followers
FROM
  panda_competitor.crawler_day_cate_analyse
WHERE par_date BETWEEN '20161201' AND '20170120';
" > ~/tmp/category_analyse.csv
#平台的相关统计
hive -e "
set hive.cli.print.header=true;
SELECT
  day_plat.par_date,
  day_plat.plat,
  day_plat.pcu,
  new_rids.activity_rids, --有效主播数
  day_plat.lives,--开播数
  day_plat.followers,
  coalesce(new_cate.new_categories,0) new_categories,--新版区数
  rid_change.new_rids --新主播数
FROM
  (
    SELECT
      par_date,
      plat,
      pcu,
      lives,
      followers,
      new_categories
    FROM
      panda_competitor.crawler_day_plat_analyse
    WHERE par_date BETWEEN '20161201' AND '20170120'
  ) day_plat
  LEFT JOIN
  (
    SELECT
      par_date,
      plat,
      count(DISTINCT rid) new_rids
    FROM panda_competitor.crawler_day_anchor_analyse
    WHERE is_new = 1
    GROUP BY par_date, plat
  ) rid_change
    ON day_plat.par_date = rid_change.par_date AND day_plat.plat = rid_change.plat
  LEFT JOIN
  (
    SELECT
      par_date,
      plat,
      count(DISTINCT rid) activity_rids
    FROM panda_competitor.crawler_anchor
    WHERE par_date BETWEEN '20161201' AND '20170120' AND task LIKE '%anchor' AND CATEGORY != '' AND populary_num > 1050
    GROUP BY par_date, plat
  ) new_rids
    ON day_plat.par_date = new_rids.par_date AND day_plat.plat = new_rids.plat
  LEFT JOIN
  (
    SELECT
      fir.par_date,
      fir.plat_name                               plat,
      coalesce(fir.cnt, 0) - coalesce(sec.cnt, 0) new_categories
    FROM
      (
        SELECT
          par_date,
          plat_name,
          count(DISTINCT trim(c_name)) cnt
        FROM panda_competitor.crawler_distinct_category
        WHERE par_date BETWEEN '20161201' AND '20170120'
        GROUP BY par_date, plat_name
      ) fir
      LEFT JOIN
      (
        SELECT
          from_unixtime(unix_timestamp(date_add(from_unixtime(unix_timestamp(par_date, 'yyyyMMdd'), 'yyyy-MM-dd'),1),'yyyy-MM-dd'),'yyyyMMdd') par_date,
          plat_name,
          cnt
        FROM
          (
            SELECT
              par_date,
              plat_name,
              count(DISTINCT trim(c_name)) cnt
            FROM panda_competitor.crawler_distinct_category
            WHERE par_date BETWEEN '20161201' AND '20170120'
            GROUP BY par_date, plat_name
          ) tmp
      ) sec
        ON fir.par_date = sec.par_date AND fir.plat_name = sec.plat_name
  ) new_cate
    ON day_plat.par_date = new_cate.par_date AND day_plat.plat = new_cate.plat;
" > ~/tmp/plat_analyse.csv