#!/bin/bash

date=$1
date=${date:=`date -d 'yesterday' +%Y%m%d`}
sub_1_days=`date -d "-1days $date" +%Y%m%d`

hive -e "
insert overwrite table panda_competitor.crawler_distinct_anchor partition(par_date)
SELECT
  coalesce(dis.rid,anc.rid) rid,
  coalesce(dis.name,anc.name) name,
  coalesce(dis.plat,anc.plat) plat,
  coalesce(dis.category,anc.category) category,
  coalesce(dis.create_time,anc.create_time) create_time,
  coalesce(dis.url,anc.url) url,
  '$date'
FROM
  (
    SELECT
      pcu_max.rid,
      time_max.name,
      pcu_max.plat,
      pcu_max.category,
      time_max.create_time,
      case when pcu_max.plat!='chushou' then
      concat(case when pcu_max.plat='douyu' then 'https://www.douyu.com/' when pcu_max.plat='huya' then 'http://www.huya.com/' when pcu_max.plat='panda' then 'http://www.panda.tv/' when pcu_max.plat='zhanqi' then 'https://www.zhanqi.tv/' when pcu_max.plat='longzhu' then 'http://star.longzhu.com/' when pcu_max.plat='quanmin' then 'http://www.quanmin.tv/v/' end,pcu_max.rid)
      else concat('http://chushou.tv/room/',pcu_max.rid,'.htm') end url
    FROM
      (
        SELECT
          rid,
          name,
          plat,
          category,
          create_time
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
                ORDER BY populary_num DESC) r
            FROM
              panda_competitor.crawler_anchor
            WHERE par_date = '$date'  and task like '%anchor' and category !=''
          ) r
        WHERE r.r = 1
      ) pcu_max
      LEFT JOIN
      (
        SELECT
          rid,
          name,
          plat,
          category,
          create_time
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
              panda_competitor.crawler_anchor
            WHERE par_date = '$date' and task like '%anchor' and category !=''
          ) r
        WHERE r.r = 1
      ) time_max
        ON pcu_max.rid = time_max.rid AND pcu_max.plat = time_max.plat
  ) dis
  FULL JOIN
  (
    SELECT
      rid,
      name,
      plat,
      category,
      create_time,
      url
    FROM panda_competitor.crawler_distinct_anchor
    WHERE par_date = '$sub_1_days'
  ) anc
    ON dis.rid = anc.rid AND dis.plat = anc.plat;
"
