#!/bin/bash

date=$1
date=${date:=`date -d 'yesterday' +%Y%m%d`}
sub_1_days=`date -d "-1days $date" +%Y%m%d`

hive -e "
insert overwrite table panda_competitor.crawler_all_plat_analyse partition(par_date)
SELECT
  plat,
  new_pcu,
  row_number()
  OVER (ORDER BY new_pcu DESC)       pcu_plat_rank,
  max_pcu,
  new_weight,
  row_number()
  OVER (ORDER BY new_weight DESC)    weight_plat_rank,
  max_weight,
  new_followers,
  row_number()
  OVER (ORDER BY new_followers DESC) fol_plat_rank,
  max_followers,
  new_live_times,
  sum_live_times,
  new_duration,
  row_number()
  OVER (ORDER BY new_duration DESC)  dur_plat_rank,
  sum_duration,
  new_rec_times,
  row_number()
  OVER (ORDER BY new_rec_times DESC) rectimes_plat_rank,
  sum_rec_times,
  lives,
  row_number()
  OVER (ORDER BY lives DESC)         lives_plat_rank,
  new_anchors,
  row_number()
  OVER (ORDER BY new_anchors DESC)   new_anchors_plat_rank,
  '$date'
FROM
  (
    SELECT
      coalesce(all_anc.plat, day_plat.plat)                                  plat,
      coalesce(day_plat.pcu, all_anc.new_pcu, 0)                             new_pcu,
      CASE WHEN coalesce(all_anc.max_pcu, 0) > coalesce(day_plat.pcu, 0)
        THEN all_anc.max_pcu
      ELSE day_plat.pcu END                                                  max_pcu,
      coalesce(day_plat.weight, all_anc.new_weight, 0)                       new_weight,
      CASE WHEN coalesce(all_anc.max_weight, 0) > coalesce(day_plat.weight, 0)
        THEN all_anc.max_weight
      ELSE day_plat.weight END                                               max_weight,
      coalesce(day_plat.followers, all_anc.new_followers, 0)                 new_followers,
      CASE WHEN coalesce(all_anc.max_followers, 0) > coalesce(day_plat.followers, 0)
        THEN all_anc.max_followers
      ELSE day_plat.followers END                                            max_followers,
      coalesce(day_plat.live_times, all_anc.new_live_times)                  new_live_times,
      coalesce(all_anc.sum_live_times, 0) + coalesce(day_plat.live_times, 0) sum_live_times,
      coalesce(day_plat.duration, all_anc.new_duration, 0)                   new_duration,
      coalesce(all_anc.sum_duration, 0) + coalesce(day_plat.duration, 0)     sum_duration,
      coalesce(day_plat.rec_times, all_anc.new_rec_times, 0)                 new_rec_times,
      coalesce(all_anc.sum_rec_times, 0) + coalesce(day_plat.rec_times, 0)   sum_rec_times,
      coalesce(day_plat.lives, 0)                                            lives,
      coalesce(day_plat.new_anchors, 0)                                      new_anchors,
      '$date'
    FROM
      (
        SELECT
          plat,
          pcu,
          live_times,
          duration,
          weight,
          followers,
          rec_times,
          lives,
          new_anchors
        FROM
          panda_competitor.crawler_day_plat_analyse
        WHERE par_date = '$date'
      ) day_plat
      FULL JOIN
      (
        SELECT
          plat,
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
          panda_competitor.crawler_all_plat_analyse
        WHERE par_date = '$sub_1_days'
      ) all_anc
        ON day_plat.plat = all_anc.plat
  ) t;
"