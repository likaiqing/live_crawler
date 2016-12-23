#!/bin/bash

date=$1
date=${date:=`date -d 'yesterday' +%Y%m%d`}


hive -e "
insert overwrite table panda_competitor_result.plat_day_report partition(par_date)
SELECT p.id, d_cate.plat, d_cate.pcu, d_cate.lives, d_cate.max_lives, d_change.activeanchors_changed, d_cate.followers, d_change.fol_changed, d_cate.weight, d_change.weight_changed, d_cate.categories, d_cate.new_categories, d_cate.reduce_categories, '$date'
FROM
  (
    SELECT plat, lives, max_lives, pcu, duration, followers, weight, rec_times, categories, new_categories, reduce_categories
    FROM
      panda_competitor.crawler_day_plat_analyse
    WHERE par_date = '$date'
  ) d_cate
  LEFT JOIN
  panda_competitor.crawler_plat p
    ON d_cate.plat = p.name
  LEFT JOIN
  (
    SELECT plat, fol_changed, weight_changed, newanchors_changed, activeanchors_changed
    FROM
      panda_competitor_result.plat_day_changed
    WHERE par_date = '$date'
  ) d_change
    ON d_cate.plat = d_change.plat;
"

hive -e "
insert overwrite table panda_competitor_result.plat_day_change_report partition(par_date)
SELECT p.id, d_cate.plat, d_cate.lives, r_change.activeanchors_rank_change, d_cate.followers, r_change.fol_rank_change, '$date'
FROM
  (
    SELECT plat, lives, followers
    FROM
      panda_competitor.crawler_day_plat_analyse
    WHERE par_date = '$date'
  ) d_cate
  LEFT JOIN
  panda_competitor.crawler_plat p
    ON d_cate.plat = p.name
  LEFT JOIN
  (
    SELECT plat, activeanchors_rank_change, fol_rank_change
    FROM
      panda_competitor_result.plat_day_change_analyse
    WHERE par_date = '$date'
  ) r_change
    ON d_cate.plat = r_change.plat;
"


hive -e "
insert overwrite table panda_competitor_result.category_day_report partition(par_date)
SELECT p.id, d_cate.plat, d_cate.category, d_cate.lives, d_change.activeanchors_changed, d_cate.pcu, d_cate.followers, d_change.fol_changed, d_cate.weight, d_change.weight_changed, d_cate.is_new, d_cate.duration, '$date'
FROM
  (
    SELECT plat, category, lives, pcu, live_times, duration, followers, weight, rec_times, is_new
    FROM
      panda_competitor.crawler_day_cate_analyse
    WHERE par_date = '$date'
  ) d_cate
  LEFT JOIN
  panda_competitor.crawler_plat p
    ON d_cate.plat = p.name
  LEFT JOIN
  (
    SELECT plat, category, fol_changed, weight_changed, activeanchors_changed
    FROM
      panda_competitor_result.category_day_changed
    WHERE par_date = '$date'
  ) d_change
    ON d_cate.plat = d_change.plat AND d_cate.category = d_change.category;
"

hive -e "
insert overwrite table panda_competitor_result.category_day_change_report partition(par_date)
SELECT p.id, d_cate.plat, d_cate.category, d_cate.lives, r_change.activeanchors_rank_change, d_cate.followers, r_change.fol_rank_change, '$date'
FROM
  (
    SELECT plat, category, lives, followers
    FROM
      panda_competitor.crawler_day_cate_analyse
    WHERE par_date = '$date'
  ) d_cate
  LEFT JOIN
  panda_competitor.crawler_plat p
    ON d_cate.plat = p.name
  LEFT JOIN
  (
    SELECT plat, category, activeanchors_rank_change, fol_rank_change
    FROM
      panda_competitor_result.category_day_change_analyse
    WHERE par_date = '$date'
  ) r_change
    ON d_cate.plat = r_change.plat AND d_cate.category = r_change.category;
"
