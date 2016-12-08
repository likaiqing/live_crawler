#!/bin/bash

date=$1
date=${date:=`date -d 'yesterday' +%Y%m%d`}
date2=`date -d "-d -1day $date" +%Y%m%d`
hive -e "
INSERT overwrite TABLE panda_result.panda_distinct_anchor_crawler PARTITION (par_date)
SELECT
  DISTINCT
  rid,
  plat,
  '$date'
FROM
  (
    SELECT
      rid,
      plat
    FROM
      panda_result.panda_distinct_anchor_crawler
    WHERE par_date = '$date2'
    UNION ALL
    SELECT
      DISTINCT
      rid,
      plat
    FROM panda_result.panda_anchor_crawler
    WHERE par_date = '$date'
  ) al;
  "

