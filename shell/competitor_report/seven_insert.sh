#!/bin/bash

date=$1
date=${date:=`date -d 'yesterday' +%Y%m%d`}
date_sub=`date -d "-1day $date" +%Y%m%d`

#主播PCU排行
hive -e "
insert overwrite table panda_competitor_result.anchor_pcu_rank partition(par_date)
SELECT
  b.id,
  a.plat,
  a.rid,
  a.name,
  a.pcu_plat_rank,
  a.pcu,
  a.weight,
  a.fol,
  nvl(cast(a.pcu_plat_rank - c.new_pcu_plat_rank AS INT), '新上榜'),
  a.category,
  a.par_date
FROM panda_competitor_result.crawler_anchor_day a
  INNER JOIN panda_competitor.crawler_plat b ON a.plat = b.name
  LEFT JOIN panda_competitor.crawler_all_anchor_analyse c
    ON c.par_date = '${date_sub}' AND a.plat = c.plat AND a.rid = c.rid AND a.category = c.category
WHERE a.par_date = '${date}'
      AND a.pcu_plat_rank >= 1 AND a.pcu_plat_rank <= 1000;
"


#主播变化趋势增减表
hive -e "
insert overwrite table panda_competitor_result.anchor_changed_rank partition(par_date)
SELECT
  b.id,
  a.plat,
  a.rid,
  a.name,
  a.category,
  a.pcu,
  nvl(cast(a.pcu_plat_rank - c.new_pcu_plat_rank AS INT), '新上榜'),
  a.livetime,
  nvl(cast(a.livetime_plat_rank - c.new_duration_plat_rank AS INT), '新上榜'),
  a.weight,
  nvl(cast(a.weight_plat_rank - c.new_weight_plat_rank AS INT), '新上榜'),
  a.par_date

FROM panda_competitor_result.crawler_anchor_day a
  INNER JOIN panda_competitor.crawler_plat b ON a.plat = b.name
  LEFT JOIN panda_competitor.crawler_all_anchor_analyse c
    ON c.par_date = '${date_sub}' AND a.plat = c.plat AND a.rid = c.rid AND a.category = c.category
WHERE a.par_date = '${date}'
      AND a.pcu_plat_rank >= 1 AND a.pcu_plat_rank <= 1000;
"


#主播订阅排行榜
hive -e "
insert overwrite table panda_competitor_result.anchor_fol_rank partition(par_date)
SELECT
  b.id,
  a.plat,
  a.rid,
  a.name,
  a.fol_plat_rank,
  a.pcu,
  a.weight,
  a.fol,
  nvl(cast(a.fol - c.new_followers AS INT), '新上榜'),
  a.category,
  a.par_date

FROM panda_competitor_result.crawler_anchor_day a
  INNER JOIN panda_competitor.crawler_plat b ON a.plat = b.name
  LEFT JOIN panda_competitor.crawler_all_anchor_analyse c
    ON c.par_date = '${date_sub}' AND a.plat = c.plat AND a.rid = c.rid AND a.category = c.category
WHERE a.par_date = '${date}'
      AND a.fol_plat_rank >= 1 AND a.fol_plat_rank <= 1000;
"

#主播订阅增长排行表
hive -e "
insert overwrite table panda_competitor_result.anchor_fol_up_rank partition(par_date)
SELECT
  b.id,
  a.plat,
  a.rid,
  a.name,
  d.fol_changed_plat_rank,
  a.pcu,
  a.weight,
  a.fol,
  d.fol_changed,
  a.category,
  a.par_date
FROM panda_competitor_result.crawler_anchor_day a
  INNER JOIN panda_competitor.crawler_plat b ON a.plat = b.name
  LEFT JOIN panda_competitor.crawler_all_anchor_analyse c
    ON c.par_date = '${date_sub}' AND a.plat = c.plat AND a.rid = c.rid AND a.category = c.category
  INNER JOIN panda_competitor_result.crawler_anchor_change_day d
    ON d.par_date = '${date}' AND a.plat = d.plat AND a.rid = d.rid
WHERE a.par_date = '${date}'
      AND d.fol_changed_plat_rank >= 1 AND d.fol_changed_plat_rank <= 1000;
"


#主播体重增长排行表
hive -e "
insert overwrite table panda_competitor_result.anchor_weight_rank partition(par_date)
SELECT
  b.id,
  a.plat,
  a.rid,
  a.name,
  a.weight_plat_rank,
  a.pcu,
  a.fol,
  c.new_weight,
  a.weight,
  a.category,
  a.par_date

FROM panda_competitor_result.crawler_anchor_day a
  INNER JOIN panda_competitor.crawler_plat b ON a.plat = b.name
  LEFT JOIN panda_competitor.crawler_all_anchor_analyse c
    ON c.par_date = '${date_sub}' AND a.plat = c.plat AND a.rid = c.rid AND a.category = c.category
WHERE a.par_date = '${date}'
      AND a.weight_plat_rank >= 1 AND a.weight_plat_rank <= 1000;
"


#主播播放时长排行表
hive -e "
insert overwrite table panda_competitor_result.anchor_livetime_rank partition(par_date)
SELECT
  b.id,
  a.plat,
  a.rid,
  a.name,
  a.livetime_plat_rank,
  a.pcu,
  a.fol,
  c.new_duration_plat_rank,
  a.category,
  a.par_date
FROM panda_competitor_result.crawler_anchor_day a
  INNER JOIN panda_competitor.crawler_plat b ON a.plat = b.name
  LEFT JOIN panda_competitor.crawler_all_anchor_analyse c
    ON c.par_date = '${date_sub}' AND a.plat = c.plat AND a.rid = c.rid AND a.category = c.category
WHERE a.par_date = '${date}'
      AND a.livetime_plat_rank >= 1 AND a.livetime_plat_rank <= 1000;
"


#推荐味日表
hive -e "
insert overwrite table panda_competitor_result.indexrec_day_report partition(par_date)
SELECT
  b.id,
  a.plat,
  a.rid,
  a.name,
  a.pcu,
  a.fol,
  d.fol_changed,
  d.weight_changed,
  a.livetime,
  e.url,
  a.par_date
FROM panda_competitor_result.crawler_anchor_day a
  INNER JOIN panda_competitor.crawler_plat b ON a.plat = b.name
  INNER JOIN panda_competitor_result.crawler_anchor_change_day d
    ON d.par_date = '${date}' AND a.plat = d.plat AND a.rid = d.rid
  LEFT JOIN panda_competitor.crawler_distinct_anchor e
    ON e.par_date = '${date}' AND a.rid = e.rid AND a.plat = e.plat AND a.category = e.category
WHERE a.par_date = '${date}'
      AND a.rectimes > 0;
"