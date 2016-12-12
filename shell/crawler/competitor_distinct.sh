#!/bin/bash

date=$1
date=${date:=`date -d 'yesterday' +%Y%m%d`}
sub_1_days=`date -d "-1days $date" +%Y%m%d`

hive -e "
insert overwrite table panda_competitor.panda_distinct_anchor partition(par_date)
SELECT
  rid,
  name,
  plat,
  category,
  create_time,
  '$date'
FROM
  (
    SELECT
      rid,
      name,
      plat,
      category,
      create_time,
      row_number()
      OVER (PARTITION BY rid, plat
        ORDER BY create_time DESC) r
    FROM
      (
        SELECT
          rid,
          name,
          plat,
          category,
          from_unixtime(unix_timestamp(par_date, 'yyyyMMdd')) create_time
        FROM panda_competitor.panda_distinct_anchor
        WHERE par_date = '$sub_1_days'
        UNION ALL
        SELECT
          DISTINCT
          rid,
          name,
          plat,
          category,
          create_time
        FROM panda_competitor.panda_douyu_anchor
        WHERE par_date = '$date'
        UNION ALL
        SELECT
          DISTINCT
          rid,
          name,
          plat,
          category,
          create_time
        FROM panda_competitor.panda_huya_anchor
        WHERE par_date = '$date'
        UNION ALL
        SELECT
          DISTINCT
          rid,
          name,
          plat,
          category,
          create_time
        FROM panda_competitor.panda_longzhu_anchor
        WHERE par_date = '$date'
        UNION ALL
        SELECT
          DISTINCT
          rid,
          name,
          plat,
          category,
          create_time
        FROM panda_competitor.panda_quanmin_anchor
        WHERE par_date = '$date'
        UNION ALL
        SELECT
          DISTINCT
          rid,
          name,
          plat,
          category,
          create_time
        FROM panda_competitor.panda_zhanqi_anchor
        WHERE par_date = '$date'
        UNION ALL
        SELECT
          DISTINCT
          rid,
          name,
          plat,
          category,
          create_time
        FROM panda_competitor.panda_chushou_anchor
        WHERE par_date = '$date'
      ) anc
  )r
WHERE r.r=1;
"
