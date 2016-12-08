#!/bin/bash

date=$1
date=${date:=`date -d 'yesterday' +%Y%m%d`}
date2=`date -d "-d -1day $date" +%Y%m%d`
hive -e "
INSERT overwrite TABLE panda_result.panda_distinct_detail_anchor_crawler PARTITION (par_date)
SELECT
  DISTINCT
  rid,
  task,
  '$date'
FROM
  (
    SELECT
      rid,
      task
    FROM
      panda_result.panda_distinct_detail_anchor_crawler
    WHERE par_date = '$date2'
    UNION ALL
    SELECT
      DISTINCT
      rid,
      task
    FROM panda_result.panda_detail_anchor_crawler
    WHERE par_date = '$date'
  ) al;
  "
