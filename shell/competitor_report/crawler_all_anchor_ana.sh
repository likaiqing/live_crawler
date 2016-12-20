#!/bin/bash

date=$1
date=${date:=`date -d 'yesterday' +%Y%m%d`}
sub_1_days=`date -d "-1days $date" +%Y%m%d`

hive -e "
insert overwrite table panda_competitor.crawler_all_anchor_analyse partition(par_date)
SELECT
  rid,
  plat,
  category,
  new_pcu,
  row_number() OVER (PARTITION BY plat ORDER BY new_pcu DESC ) pcu_plat_rank,
  row_number() OVER (PARTITION BY plat,category ORDER BY new_pcu DESC ) pcu_cate_rank,
  max_pcu,
  new_weight,
  row_number() OVER (PARTITION BY plat ORDER BY new_weight DESC ) weight_plat_rank,
  row_number() OVER (PARTITION BY plat,category ORDER BY new_weight DESC ) weight_cate_rank,
  max_weight,
  new_followers,
  row_number() OVER (PARTITION BY plat ORDER BY new_followers DESC ) fol_plat_rank,
  row_number() OVER (PARTITION BY plat,category ORDER BY new_followers DESC ) fol_cate_rank,
  max_followers,
  new_live_times,
  sum_live_times,
  new_duration,
  row_number() OVER (PARTITION BY plat ORDER BY new_duration DESC ) dur_plat_rank,
  row_number() OVER (PARTITION BY plat,category ORDER BY new_duration DESC ) dur_cate_rank,
  sum_duration,
  new_rec_times,
  row_number() OVER (PARTITION BY plat ORDER BY new_rec_times DESC ) rectimes_plat_rank,
  row_number() OVER (PARTITION BY plat,category ORDER BY new_rec_times DESC ) rectimes_cate_rank,
  sum_rec_times,
  is_new,
  '$date'
FROM
  (
    SELECT
      coalesce(all_anc.rid, day_anc.rid)                                    rid,
      coalesce(all_anc.plat, day_anc.plat)                                  plat,
      coalesce(all_anc.category, day_anc.category)                          category,
      coalesce(day_anc.pcu, all_anc.new_pcu, 0)                             new_pcu,
      CASE WHEN coalesce(all_anc.max_pcu, 0) > coalesce(day_anc.pcu, 0)
        THEN all_anc.max_pcu
      ELSE day_anc.pcu END                                                  max_pcu,
      coalesce(day_anc.weight, all_anc.new_weight, 0)                       new_weight,
      CASE WHEN coalesce(all_anc.max_weight, 0) > coalesce(day_anc.weight, 0)
        THEN all_anc.max_weight
      ELSE day_anc.weight END                                               max_weight,
      coalesce(day_anc.followers, all_anc.new_followers, 0)                 new_followers,
      CASE WHEN coalesce(all_anc.max_followers, 0) > coalesce(day_anc.followers, 0)
        THEN all_anc.max_followers
      ELSE day_anc.followers END                                            max_followers,
      coalesce(day_anc.live_times, all_anc.new_live_times)                  new_live_times,
      coalesce(all_anc.sum_live_times, 0) + coalesce(day_anc.live_times, 0) sum_live_times,
      coalesce(day_anc.duration, all_anc.new_duration, 0)                   new_duration,
      coalesce(all_anc.sum_duration, 0) + coalesce(day_anc.duration, 0)     sum_duration,
      coalesce(day_anc.rec_times, all_anc.new_rec_times, 0)                 new_rec_times,
      coalesce(all_anc.sum_rec_times, 0) + coalesce(day_anc.rec_times, 0)   sum_rec_times,
      CASE WHEN dis.rid IS NULL
        THEN 1
      ELSE 0 END                                                            is_new
    FROM
      (
        SELECT
          rid,
          plat,
          category,
          pcu,
          live_times,
          duration,
          weight,
          followers,
          rec_times
        FROM
          panda_competitor.crawler_day_anchor_analyse
        WHERE par_date = '$date'
      ) day_anc
      FULL JOIN
      (
        SELECT
          rid,
          plat,
          category,
          new_pcu,
          max_pcu,
          new_weight,
          max_weight,
          new_followers,
          max_followers,
          new_live_times,
          sum_live_times,
          new_duration,
          sum_duration,
          new_rec_times,
          sum_rec_times
        FROM
          panda_competitor.crawler_all_anchor_analyse
        WHERE par_date = '$sub_1_days'
      ) all_anc
        ON day_anc.rid = all_anc.rid AND day_anc.plat = all_anc.plat AND day_anc.category = all_anc.category
      LEFT JOIN
      (
        SELECT DISTINCT
          rid,
          plat
        FROM panda_competitor.crawler_distinct_anchor
        WHERE par_date = '$sub_1_days'
      ) dis
        ON day_anc.rid = dis.rid AND day_anc.plat = dis.plat
  )t;
"