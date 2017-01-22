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
  nvl(cast(a.pcu_plat_rank - c.pcu_rank AS INT), '新上榜'),
  a.room_content,
  a.par_date
FROM panda_competitor_result.crawler_anchor_day a
  INNER JOIN panda_competitor.crawler_plat b ON a.plat = b.name
  LEFT JOIN panda_competitor_result.plat_anchor_rank c
    ON c.par_date = '${date_sub}' AND a.plat = c.plat AND a.rid = c.rid
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
  nvl(cast(a.pcu_plat_rank - c.pcu_rank AS INT), '新上榜'),
  a.livetime,
  nvl(cast(a.livetime_plat_rank - c.duration_rank AS INT), '新上榜'),
  a.weight,
  nvl(cast(a.weight_plat_rank - c.weight_rank AS INT), '新上榜'),
  a.fol,
  a.par_date

FROM panda_competitor_result.crawler_anchor_day a
  INNER JOIN panda_competitor.crawler_plat b ON a.plat = b.name
  LEFT JOIN panda_competitor_result.plat_anchor_rank c
    ON c.par_date = '${date_sub}' AND a.plat = c.plat AND a.rid = c.rid
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
  nvl(cast(a.fol - c.followers AS INT), '新上榜'),
  a.room_content,
  a.par_date

FROM panda_competitor_result.crawler_anchor_day a
  INNER JOIN panda_competitor.crawler_plat b ON a.plat = b.name
  LEFT JOIN  panda_competitor_result.plat_anchor_rank c
    ON c.par_date = '${date_sub}' AND a.plat = c.plat AND a.rid = c.rid
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
  a.room_content,
  a.par_date
FROM panda_competitor_result.crawler_anchor_day a
  INNER JOIN panda_competitor.crawler_plat b ON a.plat = b.name
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
  c.weight,
  a.weight,
  a.room_content,
  a.par_date

FROM panda_competitor_result.crawler_anchor_day a
  INNER JOIN panda_competitor.crawler_plat b ON a.plat = b.name
  LEFT JOIN panda_competitor_result.plat_anchor_rank c
    ON c.par_date = '${date_sub}' AND a.plat = c.plat AND a.rid = c.rid
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
  a.livetime,
  a.room_content,
  a.par_date
FROM panda_competitor_result.crawler_anchor_day a
  INNER JOIN panda_competitor.crawler_plat b ON a.plat = b.name
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

#################################################################################################
#2017-01-18新增
#####
#综合排名中间表

hive -e "
insert overwrite table panda_competitor_result.anchor_m_comprehensive_rank partition(par_date)

select
case when d.rid is null then c.rid else d.rid end,
case when d.plat is null then c.plat else d.plat end,
case when d.comprehensive_score is null then c.comprehensive_score else d.comprehensive_score end,
case when d.comprehensive_rank is null then c.comprehensive_rank else d.comprehensive_rank end,

case when d.grow_score is null then c.grow_score else d.grow_score end,
case when d.grow_rank is null then c.grow_rank else d.grow_rank end,

'${date}'

from(

select a.rid,
a.plat,

(a.pcu/e.pcu_p)*e.pcu_w+
(1-a.livetime/e.livetime_p)*e.livetime_w+
(a.weight/e.weight_p)*e.weight_w+
(a.fol/e.fol_p)*e.fol_w as comprehensive_score,

row_number()over(
partition by a.plat
order by
--
cast((a.pcu/e.pcu_p)*e.pcu_w+
(1-a.livetime/e.livetime_p)*e.livetime_w+
(a.weight/e.weight_p)*e.weight_w+
(a.fol/e.fol_p)*e.fol_w as int) desc
) as comprehensive_rank,


((a.pcu-nvl(c.pcu,0))/f.pcu_p)*f.pcu_w+
((a.weight-nvl(c.weight,0))/f.weight_p)*f.weight_w+
((a.fol-nvl(c.followers,0))/f.fol_p)*f.fol_w grow_score,--总分

row_number()over(
partition by b.id
order by
--
cast(((a.pcu-nvl(c.pcu,0))/f.pcu_p)*f.pcu_w+
((a.weight-nvl(c.weight,0))/f.weight_p)*f.weight_w+
((a.fol-nvl(c.followers,0))/f.fol_p)*f.fol_w as int) desc
) grow_rank,

a.par_date


FROM panda_competitor_result.crawler_anchor_day a
  INNER JOIN panda_competitor.crawler_plat b ON a.plat = b.name
  LEFT JOIN panda_competitor_result.plat_anchor_rank c
    ON c.par_date = '${date_sub}' AND a.plat = c.plat AND a.rid = c.rid
  left join
  (select * from panda_competitor_result.anchor_live_parameter where type=0) e
  left join
  (select * from panda_competitor_result.anchor_live_parameter where type=1) f
WHERE a.par_date = '${date}') d

full outer join
(select * from panda_competitor_result.anchor_m_comprehensive_rank where par_date='${date_sub}' ) c
on d.plat=c.plat and d.rid=c.rid;
"





 #主播综合排名
hive -e "
insert overwrite table panda_competitor_result.anchor_comprehensive_rank partition(par_date)
select
--排名
row_number()over(
partition by b.id
order by
--
cast((a.pcu/e.pcu_p)*e.pcu_w+
(1-a.livetime/e.livetime_p)*e.livetime_w+
(a.weight/e.weight_p)*e.weight_w+
(a.fol/e.fol_p)*e.fol_w as int) desc
--
),--总分排序

------

b.id,--平台id
a.plat,--平台名称
a.rid,--主播id
a.name,--主播名字
a.pcu,--pcu
a.livetime,--开播时长


cast(a.fol - nvl(c.followers,0) AS INT),--订阅增长
cast(a.weight-nvl(c.weight,0)as int),--体重增量

(a.pcu/e.pcu_p)*e.pcu_w,--pcu分数
(1-a.livetime/e.livetime_p)*e.livetime_w,--开播时长分数

(a.weight/e.weight_p)*e.weight_w,--体重增量分数
(a.fol/e.fol_p)*e.fol_w,--订阅增量分数


--总分
(a.pcu/e.pcu_p)*e.pcu_w+
(1-a.livetime/e.livetime_p)*e.livetime_w+
(a.weight/e.weight_p)*e.weight_w+
(a.fol/e.fol_p)*e.fol_w,

row_number()over(
partition by b.id
order by
--
cast((a.pcu/e.pcu_p)*e.pcu_w+
(1-a.livetime/e.livetime_p)*e.livetime_w+
(a.weight/e.weight_p)*e.weight_w+
(a.fol/e.fol_p)*e.fol_w as int) desc
--
) -nvl(g.comprehensive_rank,0), --名次变化


a.par_date
FROM panda_competitor_result.crawler_anchor_day a
  INNER JOIN panda_competitor.crawler_plat b ON a.plat = b.name
  LEFT JOIN panda_competitor_result.plat_anchor_rank c
    ON c.par_date = '${date_sub}' AND a.plat = c.plat AND a.rid = c.rid
  left join
  (select * from panda_competitor_result.anchor_live_parameter where type=0) e
  left join
  (select * from panda_competitor_result.anchor_live_parameter where type=1) f
  left join
  (select * from panda_competitor_result.anchor_m_comprehensive_rank where par_date='${date_sub}') g
on a.plat=g.plat and a.rid=g.rid
WHERE a.par_date = '${date}';
"


#主播成长排名
hive -e "
insert overwrite table panda_competitor_result.anchor_growth_rank partition(par_date)
select
row_number()over(
partition by b.id
order by
--
cast(((a.pcu-nvl(c.pcu,0))/e.pcu_p)*e.pcu_w+
((a.weight-nvl(c.weight,0))/e.weight_p)*e.weight_w+
((a.fol-nvl(c.followers,0))/e.fol_p)*e.fol_w as int) desc
--
),--排序


b.id,--平台id
a.plat,--平台名称
a.rid,--主播id
a.name,--主播名称
a.pcu-nvl(c.pcu,0),--pcu增值
a.fol-nvl(c.followers,0),--订阅增值
a.weight-nvl(c.weight,0),--体重增值
((a.pcu-nvl(c.pcu,0))/e.pcu_p)*e.pcu_w,--pcu增值分数


case when a.pcu=(a.pcu-nvl(c.pcu,0)) then 1 else 0 end,--pcu增值状态



((a.weight-nvl(c.weight,0))/e.weight_p)*e.weight_w,--体重增量分数
((a.fol-nvl(c.followers,0))/e.fol_p)*e.fol_w,--订阅增量分数

((a.pcu-nvl(c.pcu,0))/e.pcu_p)*e.pcu_w+
((a.weight-nvl(c.weight,0))/e.weight_p)*e.weight_w+
((a.fol-nvl(c.followers,0))/e.fol_p)*e.fol_w,--总分

row_number()over(
partition by b.id
order by
--
cast(((a.pcu-nvl(c.pcu,0))/e.pcu_p)*e.pcu_w+
((a.weight-nvl(c.weight,0))/e.weight_p)*e.weight_w+
((a.fol-nvl(c.followers,0))/e.fol_p)*e.fol_w as int) desc
--
)-nvl(f.grow_rank,0),--名次变化
a.par_date
FROM panda_competitor_result.crawler_anchor_day a
  INNER JOIN panda_competitor.crawler_plat b ON a.plat = b.name
  LEFT JOIN panda_competitor_result.plat_anchor_rank c
    ON c.par_date = '${date_sub}' AND a.plat = c.plat AND a.rid = c.rid
  left join
  (select * from panda_competitor_result.anchor_live_parameter where type=1) e
  left join
  (select * from panda_competitor_result.anchor_m_comprehensive_rank where par_date='${date_sub}') f
on a.plat=f.plat and a.rid=f.rid
WHERE a.par_date = '${date}';"

#####生成excel
#成长
/usr/local/jdk1.8.0_60/bin/java -jar /home/likaiqing/hive-tool/export2excel.jar $date panda_competitor_result.anchor_comprehensive_rank par_date,rank,plat_id,plat,rid,name,puu,livetime,fol_up,weight_up,pcu_score,livetime_score,weight_score,fol_score,total_score,rank_change "日期,排序,平台ID,平台名称,主播ID,主播名称,PCU,开播时长,订阅增量,体重增量,PCU分数,开播时长分数,体重增量分数,订阅增量分数,总分,名次变化" /data/tmp/zhengbo/file/
#增量
/usr/local/jdk1.8.0_60/bin/java -jar /home/likaiqing/hive-tool/export2excel.jar $date panda_competitor_result.anchor_growth_rank par_date,rank,plat_id,plat,rid,name,pcu_up,fol_up,weight_up,pcu_score,pcu_type,weight_up_score,fol_up_score,total_score,rank_change "日期,排序,平台ID,平台名称,主播ID,主播名称,PCU增值,订阅增量,体重增量,PCU增值分数,PCU增值状态,体重增量分数,订阅增量分数,总分,名次变化" /data/tmp/zhengbo/file/


#发送邮件
#
/usr/local/jdk1.8.0_60/bin/java -jar /home/likaiqing/hive-tool/send_mail.jar "主播成长" "内容见附件" /data/tmp/zhengbo/file/anchor_comprehensive_rank${date}.xlsx "zhengbo@panda.tv" "baimuhai@panda.tv,lushenggang@panda.tv,wangshuo@panda.tv,likaiqing@panda.tv,zhaolirong@panda.tv"

#
/usr/local/jdk1.8.0_60/bin/java -jar /home/likaiqing/hive-tool/send_mail.jar "主播综合" "内容见附件" /data/tmp/zhengbo/file/anchor_growth_rank${date}.xlsx "zhengbo@panda.tv" "baimuhai@panda.tv,lushenggang@panda.tv,wangshuo@panda.tv,likaiqing@panda.tv,zhaolirong@panda.tv"


