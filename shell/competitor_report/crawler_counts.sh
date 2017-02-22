#!/bin/bash

date=$1
date=${date:=`date -d 'yesterday' +%Y%m%d`}
sub_3_days=`date -d "-2 day ${date}" +%Y%m%d`

hive -e "
insert overwrite table panda_competitor_result.crawler_counts partition(par_date)
SELECT
  task,
  count(DISTINCT task_random),
  par_date,
  '$date'
FROM panda_competitor.crawler_anchor
WHERE par_date BETWEEN '$sub_3_days' AND '$date' AND task rlike '.*anchor'
GROUP BY par_date, task
UNION ALL
SELECT
  task,
  count(DISTINCT task_random),
  par_date,
  '$date'
FROM panda_competitor.crawler_detail_anchor
WHERE par_date BETWEEN '$sub_3_days' AND '$date' AND task rlike '.*detailanchor'
GROUP BY par_date, task
UNION ALL
SELECT
  task,
  count(DISTINCT task_random),
  par_date,
  '$date'
FROM panda_competitor.crawler_indexrec_detail_anchor
WHERE par_date BETWEEN '$sub_3_days' AND '$date' AND task rlike '.*indexrec'
GROUP BY par_date, task;
"