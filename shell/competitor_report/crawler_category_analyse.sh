#!/bin/bash

date=$1
date=${date:=`date -d 'yesterday' +%Y%m%d`}
sub_1_days=`date -d "-1day $date" +%Y%m%d`

#hive -e "
#insert overwrite table panda_competitor_result.category_day_analyse partition(par_date)
#SELECT
#  plat,
#  category,
#  pcu,
#  row_number()
#  OVER (PARTITION BY plat
#    ORDER BY pcu DESC)         pcu_rank,
#  duration                     duration,
#  row_number()
#  OVER (PARTITION BY plat
#    ORDER BY duration DESC)    duration_rank,
#  rec_times                    rectimes,
#  row_number()
#  OVER (PARTITION BY plat
#    ORDER BY rec_times DESC)   rectimes_rank,
#  followers                    fol,
#  row_number()
#  OVER (PARTITION BY plat
#    ORDER BY followers DESC)   fol_rank,
#  weight                       weight,
#  row_number()
#  OVER (PARTITION BY plat
#    ORDER BY weight DESC)      weight_rank,
#  0                            gift_users,
#  0                            gift_times,
#  0.0                          gift_value,
#  0                            giftvalue_rank,
#  0                            barrage_users,
#  0                            barrage_amount,
#  0                            barrage_rank,
#  new_anchors,
#  row_number()
#  OVER (PARTITION BY plat
#    ORDER BY new_anchors DESC) new_anchors_rank,
#  lives,
#  row_number()
#  OVER (PARTITION BY plat
#    ORDER BY lives DESC)       lives_rank,
#  '$date'
#FROM
#  panda_competitor.crawler_day_cate_analyse
#WHERE par_date = '$date';
#"

hive -e "
insert overwrite table panda_competitor_result.category_day_changed partition(par_date)
SELECT
  c1.plat,
  c1.category,
  c1.pcu - coalesce(c2.new_pcu, 0)                              pcu_changed,
  row_number()
  OVER (PARTITION BY c1.plat
    ORDER BY c1.pcu - coalesce(c2.new_pcu, 0) DESC)             pcu_changed_rank,
  c1.duration - coalesce(c2.new_duration, 0.0)                  duration_changed,
  row_number()
  OVER (PARTITION BY c1.plat
    ORDER BY c1.duration - coalesce(c2.new_duration, 0.0) DESC) duration_change_rank,
  c1.rec_times - coalesce(c2.new_rec_times, 0)                  rectimes_changed,
  row_number()
  OVER (PARTITION BY c1.plat
    ORDER BY c1.rec_times - coalesce(c2.new_rec_times, 0) DESC) rectimes_change_rank,
  c1.followers - coalesce(c2.new_followers, 0)                  fol_chaned,
  row_number()
  OVER (PARTITION BY c1.plat
    ORDER BY c1.followers - coalesce(c2.new_followers, 0) DESC) fol_change_rank,
  c1.weight - coalesce(c2.new_weight, 0.0)                      weight_changed,
  row_number()
  OVER (PARTITION BY c1.plat
    ORDER BY c1.weight - coalesce(c2.new_weight, 0.0) DESC)     weight_change_rank,
  0.0,
  0,
  0,
  0,
  c1.lives - coalesce(c2.lives, 0)                              activeanchors_changed,
  row_number()
  OVER (PARTITION BY c1.plat
    ORDER BY c1.lives - coalesce(c2.lives, 0) DESC)             lives_change_rank,
  c1.new_anchors - coalesce(c2.new_anchors, 0)                  newanchors_changed,
  row_number()
  OVER (PARTITION BY c1.plat
    ORDER BY c1.new_anchors - coalesce(c2.new_anchors, 0) DESC) newanchors_change_rank,
  '$date'
FROM
  (
    SELECT
      plat,
      category,
      pcu,
      duration,
      rec_times,
      followers,
      weight,
      0.0,
      0,
      lives,
      new_anchors
    FROM
      panda_competitor.crawler_day_cate_analyse
    WHERE par_date = '$date'
  ) c1
  LEFT JOIN
  (
    SELECT
      plat,
      category,
      new_pcu,
      new_duration,
      new_rec_times,
      new_followers,
      new_weight,
      0.0,
      0,
      lives,
      new_anchors
    FROM
      panda_competitor.crawler_all_cate_analyse
    WHERE par_date = '$sub_1_days'
  ) c2
    ON c1.plat = c2.plat AND c1.category = c2.category;
"

hive -e "
insert overwrite table panda_competitor_result.category_day_change_analyse partition(par_date)
SELECT
  c1.plat,
  c1.category,
  c1.new_pcu_rank-coalesce(c2.new_pcu_rank,0) pcu_rank_changed,
  row_number() OVER (PARTITION BY c1.plat ORDER BY c1.new_pcu_rank-coalesce(c2.new_pcu_rank,0) DESC ) pcu_rank_change_rank,
  c1.new_duration_rank-coalesce(c2.new_duration_rank,0) dur_rank_changed,
  row_number() OVER (PARTITION BY c1.plat ORDER BY c1.new_duration_rank-coalesce(c2.new_duration_rank,0) DESC ) dur_rank_change_rank,
  c1.new_rec_times_rank-coalesce(c2.new_rec_times_rank,0) rec_times_rank_changed,
  row_number() OVER (PARTITION BY c1.plat ORDER BY c1.new_rec_times_rank-coalesce(c2.new_rec_times_rank,0) DESC ) rec_times_rank_change_rank,
  c1.new_followers_rank-coalesce(c2.new_followers_rank,0) fol_rank_changed,
  row_number() OVER (PARTITION BY c1.plat ORDER BY c1.new_followers_rank-coalesce(c2.new_followers_rank,0) DESC ) fol_rank_change_rank,
  c1.new_weight_rank-coalesce(c2.new_weight_rank,0) weight_rank_changed,
  row_number() OVER (PARTITION BY c1.plat ORDER BY c1.new_weight_rank-coalesce(c2.new_weight_rank,0) DESC ) weight_rank_change_rank,
  0,
  0,
  c1.new_anchors_rank-coalesce(c2.new_anchors_rank,0) new_anchors_rank_changed,
  row_number() OVER (PARTITION BY c1.plat ORDER BY c1.new_anchors_rank-coalesce(c2.new_anchors_rank,0) DESC ) new_anchors_rank_change_rank,
  c1.lives_rank-coalesce(c2.lives_rank,0) lives_rank,
  row_number() OVER (PARTITION BY c1.plat ORDER BY c1.lives_rank-coalesce(c2.lives_rank,0) DESC ) lives_rank_change_rank,
  '$date'
FROM
  (
    SELECT
      plat,
      category,
      new_pcu_rank,
      new_duration_rank,
      new_rec_times_rank,
      new_followers_rank,
      new_weight_rank,
      new_anchors_rank,
      lives_rank
    FROM
      panda_competitor.crawler_all_cate_analyse
    WHERE par_date = '$date'
  ) c1
  LEFT JOIN
  (
    SELECT
      plat,
      category,
      new_pcu_rank,
      new_duration_rank,
      new_rec_times_rank,
      new_followers_rank,
      new_weight_rank,
      new_anchors_rank,
      lives_rank
    FROM
      panda_competitor.crawler_all_cate_analyse
    WHERE par_date = '$sub_1_days'
  ) c2
  ON c1.plat=c2.plat AND c1.category=c2.category;
"