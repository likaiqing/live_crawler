#!/bin/bash

date=$1
date=${date:=`date -d "yesterday" +%Y%m%d`}

hive -e "
insert overwrite table panda_competitor_result.anchor_day_changed_analyse_by_sameday partition(par_date)
SELECT
  change.rid,
  dis.name,
  change.plat,
  change.category,
  pcu.pcu,
  change.follower_num,
  change.weight_num,
  CASE WHEN (abs(change.follow_change) / pcu.pcu > 1.4) OR (change.follow_change < -10000)
    THEN 0
  ELSE change.follow_change END follow_change,
  change.weight_change,
  dis.title,
  '$date'
FROM
  (
    SELECT
      r_asc.rid,
      r_asc.plat,
      r_asc.category,
      r_desc.follower_num,
      r_desc.weight_num,
      r_desc.follower_num - r_asc.follower_num follow_change,
      r_desc.weight_num - r_asc.weight_num     weight_change
    FROM
      (
        SELECT
          rid,
          plat,
          category,
          follower_num,
          weight_num
        FROM
          (
            SELECT
              rid,
              split(task,'detail')[0] plat,
              category_sec category,
              follower_num,
              weight_num,
              row_number()
              OVER (PARTITION BY rid, split(task,'detail')[0], category_sec
                ORDER BY create_time) r_asc
            FROM
              panda_competitor.crawler_detail_anchor
            WHERE par_date = '$date' AND (task = 'douyudetailanchor' OR task = 'huyadetailanchor')
          ) r_asc
        WHERE r_asc = 1
      ) r_asc
      JOIN
      (
        SELECT
          rid,
          plat,
          category,
          follower_num,
          weight_num
        FROM
          (
            SELECT
              rid,
              split(task,'detail')[0] plat,
              category_sec category,
              follower_num,
              weight_num,
              row_number()
              OVER (PARTITION BY rid, split(task,'detail')[0], category_sec
                ORDER BY create_time DESC) r_asc
            FROM
              panda_competitor.crawler_detail_anchor
            WHERE par_date = '$date' AND (task = 'douyudetailanchor' OR task = 'huyadetailanchor')
          ) r_asc
        WHERE r_asc = 1
      ) r_desc
        ON r_asc.rid = r_desc.rid AND r_asc.plat = r_desc.plat AND r_asc.category = r_desc.category
  ) change
  LEFT JOIN
  (
    SELECT
      rid,
      split(task,'detail')[0] plat,
      category_sec category,
      max(online_num) pcu
    FROM
      panda_competitor.crawler_detail_anchor
    WHERE par_date = '$date' AND (task = 'douyudetailanchor' OR task = 'huyadetailanchor')
    GROUP BY rid,split(task,'detail')[0] ,category_sec
  ) pcu
    ON change.rid = pcu.rid AND change.plat = pcu.plat AND change.category = pcu.category
  LEFT JOIN
  (
    SELECT
      rid,
      plat,
      category,
      name,
      title
    FROM
      panda_competitor.crawler_distinct_anchor
    WHERE par_date = '$date'
  ) dis
    ON change.rid = dis.rid AND change.plat = dis.plat AND change.category = dis.category;
"