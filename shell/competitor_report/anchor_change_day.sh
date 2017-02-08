#!/bin/bash

date=$1
date=${date:=`date -d 'yesterday' +%Y%m%d`}
date_sub=`date -d "-1day $date" +%Y%m%d`

#主播日数据
hive -e "
insert overwrite table panda_competitor_result.crawler_anchor_day partition(par_date)
SELECT
  plat,
  category,
  rid,
  name,
  --主播昵称
  pcu,
  row_number()
  OVER (PARTITION BY plat
    ORDER BY pcu DESC),
  row_number()
  OVER (PARTITION BY plat, category
    ORDER BY pcu DESC),
  duration,
  row_number()
  OVER (PARTITION BY plat
    ORDER BY duration DESC),
  row_number()
  OVER (PARTITION BY plat, category
    ORDER BY duration DESC),
  rec_times,
  row_number()
  OVER (PARTITION BY plat
    ORDER BY rec_times DESC),
  row_number()
  OVER (PARTITION BY plat, category
    ORDER BY rec_times DESC),
  followers,
  row_number()
  OVER (PARTITION BY plat
    ORDER BY followers DESC),
  row_number()
  OVER (PARTITION BY plat, category
    ORDER BY followers DESC),
  weight,
  row_number()
  OVER (PARTITION BY plat
    ORDER BY weight DESC),
  row_number()
  OVER (PARTITION BY plat, category
    ORDER BY weight DESC),
  NULL,
  --gift_users  赠送礼物人数
  NULL,
  ---gift_times 赠送礼物次数
  NULL,
  --gift_value  赠送礼物价值
  NULL,
  --giftvalue_plat_rank 平台礼物价值排名
  NULL,
  --giftvalue_cate_rank 版区礼物价值排名
  NULL,
  --barrage_users   发弹幕人数
  NULL,
  --barrage_amount  发弹幕数量
  NULL,
  --barrage_plat_rank   平台弹幕数排名
  NULL,
  --barrage_cate_rank   版区弹幕数排名
  is_new,
  --是否是新主播
  room_content,
  par_date
FROM (
       SELECT
         a.par_date,
         a.plat,
         c.category,
         a.rid,
         b.name,
         b.room_content,
         min(a.is_new)    AS is_new,
         max(a.pcu)       AS pcu,
         sum(a.duration)  AS duration,
         max(a.rec_times) AS rec_times,
         max(a.followers) AS followers,
         max(a.weight)    AS weight
       FROM panda_competitor.crawler_day_anchor_analyse a
         LEFT JOIN
         (select rid,plat,name,title as room_content,
row_number() over(partition by rid,plat order by create_time desc) as rw 
from panda_competitor.crawler_anchor
where par_date='${date}'
and (rid is not null and rid <>'')) b
           ON b.rw=1 and a.rid = b.rid AND a.plat = b.plat
         LEFT JOIN
         (SELECT
            plat,
            rid,
            category,
            pcu,
            row_number()
            OVER (PARTITION BY plat, rid
              ORDER BY pcu DESC) AS rw
          FROM panda_competitor.crawler_day_anchor_analyse
          WHERE par_date = '${date}') c
           ON a.plat = c.plat AND a.rid = c.rid AND c.rw = 1
       WHERE a.par_date = '${date}'
       GROUP BY a.par_date, a.plat, c.category, a.rid, b.name,b.room_content
     ) zz;"


hive -e "
insert overwrite table panda_competitor_result.crawler_anchor_change_day partition(par_date)
SELECT
  aa.plat,
  --    平台
  aa.category,
  --    版区
  aa.rid,
  --主播ID
  aa.name,
  --主播昵称
  (aa.pcu - nvl(bb.pcu, 0)),
  --pcu_changed PCU变化量
  row_number()
  OVER (PARTITION BY aa.plat
    ORDER BY (aa.pcu - nvl(bb.pcu, 0)) DESC),
  --pcu_changed_plat_rank
  row_number()
  OVER (PARTITION BY aa.plat, aa.category
    ORDER BY (aa.pcu - nvl(bb.pcu, 0)) DESC),
  --pcu_changed_cate_rank
  (aa.duration - nvl(bb.duration, 0)),
  --livetime_changed    直播时长变化量
  row_number()
  OVER (PARTITION BY aa.plat
    ORDER BY (aa.duration - nvl(bb.duration, 0)) DESC),
  --livetime_changed_plat_rank
  row_number()
  OVER (PARTITION BY aa.plat, aa.category
    ORDER BY (aa.duration - nvl(bb.duration, 0)) DESC),
  --livetime_changed_cate_rank
  (aa.rec_times - nvl(bb.rec_times, 0)),
  --rec_changed 推荐次数变化量
  row_number()
  OVER (PARTITION BY aa.plat
    ORDER BY (aa.rec_times - nvl(bb.rec_times, 0)) DESC),
  --rec_changed_plat_rank
  row_number()
  OVER (PARTITION BY aa.plat, aa.category
    ORDER BY (aa.rec_times - nvl(bb.rec_times, 0)) DESC),
  --rec_changed_cate_rank
  (aa.followers - nvl(bb.followers, 0)),
  --fol_changed 关注变化量
  row_number()
  OVER (PARTITION BY aa.plat
    ORDER BY (aa.followers - nvl(bb.followers, 0)) DESC),
  --fol_changed_plat_rank
  row_number()
  OVER (PARTITION BY aa.plat, aa.category
    ORDER BY (aa.followers - nvl(bb.followers, 0)) DESC),
  --fol_changed_cate_rank
  (aa.weight - nvl(bb.weight, 0)),
  --weight_changed  体重变化量
  row_number()
  OVER (PARTITION BY aa.plat
    ORDER BY (aa.weight - nvl(bb.weight, 0)) DESC),
  --weight_changed_plat_rank
  row_number()
  OVER (PARTITION BY aa.plat, aa.category
    ORDER BY (aa.weight - nvl(bb.weight, 0)) DESC),
  --weight_changed_cate_rank
  NULL,
  --giftvalue_changed   礼物价值变化量
  NULL,
  --giftvalue_changed_plat_rank
  NULL,
  --giftvalue_changed_cate_rank
  NULL,
  --barrage_changed 弹幕数变化量
  NULL,
  --barrage_changed_plat_rank
  NULL,
  --barrage_changed_cate_rank
  room_content,
  aa.par_date
FROM

  (SELECT
     a.par_date,
     a.plat,
     c.category,
     a.rid,
     b.name,
     b.room_content,
     max(a.pcu)       AS pcu,
     sum(a.duration)  AS duration,
     max(a.rec_times) AS rec_times,
     max(a.followers) AS followers,
     max(a.weight)    AS weight
   FROM panda_competitor.crawler_day_anchor_analyse a
     LEFT JOIN
     (select rid,plat,name,title as room_content,
row_number() over(partition by rid,plat order by create_time desc) as rw 
from panda_competitor.crawler_distinct_anchor 
where par_date='${date}') b
           ON b.rw=1 and a.rid = b.rid AND a.plat = b.plat
     LEFT JOIN
     (SELECT
        plat,
        rid,
        category,
        pcu,
        row_number()
        OVER (PARTITION BY plat, rid
          ORDER BY pcu DESC) AS rw
      FROM panda_competitor.crawler_day_anchor_analyse
      WHERE par_date = '${date}') c
       ON a.plat = c.plat AND a.rid = c.rid AND c.rw = 1
   WHERE par_date = '${date}'
   GROUP BY a.par_date, a.plat, c.category, a.rid, b.name,b.room_content) aa
  LEFT JOIN
  (

select 
par_date,
plat,
rid,
pcu,
duration,
rec_times,
followers,
weight
from panda_competitor_result.plat_anchor_rank
where  par_date= '${date_sub}' 
  ) bb
    ON aa.plat = bb.plat AND aa.rid = bb.rid;
"


hive -e "
insert overwrite table panda_competitor_result.plat_anchor_rank partition(par_date)
select case when b.rid is not null then b.rid else c.rid end as rid,
case when b.plat is not null then b.plat else c.plat end as plat,
case when b.pcu is not null then b.pcu else c.pcu end,
case when b.pcu_rank is not null then b.pcu_rank else c.pcu_rank end,
case when b.duration is not null then b.duration else c.duration end, 
case when b.duration_rank is not null then b.duration_rank else c.duration_rank end,
case when b.weight is not null then b.weight else c.weight end,
case when b.weight_rank is not null then b.weight_rank else c.weight_rank end,
case when b.followers is not null then b.followers else c.followers end,
case when b.followers_rank is not null then b.followers_rank else c.followers_rank end,
case when b.rec_times is not null then b.rec_times else c.rec_times end,
case when b.rec_times_rank is not null then b.rec_times_rank else c.rec_times_rank end,
'${date}'
from (
select rid,
plat,
pcu,
row_number()over(partition by plat order by pcu desc) as pcu_rank,
duration,
row_number()over(partition by plat order by duration desc) as duration_rank,
weight,
row_number()over(partition by plat order by weight desc) as weight_rank,
followers,
row_number()over(partition by plat order by followers desc) as followers_rank,
rec_times,
row_number()over(partition by plat order by rec_times desc) as rec_times_rank,
'${date}' 
from(
select rid,
plat,
max(pcu) as pcu,
sum(duration) duration,
max(weight) as weight,
max(followers)as followers,
max(rec_times) as rec_times
from panda_competitor.crawler_day_anchor_analyse
where par_date='${date}'
group by rid,plat
) a) b
full outer join( 
select * from panda_competitor_result.plat_anchor_rank where par_date='${date_sub}'
) c
on b.plat=c.plat and c.rid=b.rid 
;"
