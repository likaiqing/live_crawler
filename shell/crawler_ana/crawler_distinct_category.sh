#!/bin/bash

date=$1
date=${date:=`date -d 'yesterday' +%Y%m%d`}
date2=`date -d "-d -1day $date" +%Y%m%d`
hive -e "
INSERT overwrite TABLE panda_competitor.crawler_distinct_category PARTITION (par_date)
SELECT
  DISTINCT
  plat_id,
  plat_name,
  c_name,
  '$date'
FROM
  (
    SELECT
      plat_id,
      plat_name,
      c_name
    FROM panda_competitor.crawler_distinct_category
    WHERE par_date = '$date2'
    UNION ALL
    SELECT
      plat_id,
      plat_name,
      trim(c_name) c_name
    FROM panda_competitor.crawler_category
    WHERE par_date = '$date'
  ) al;
  "

