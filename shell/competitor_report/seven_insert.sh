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
(a.fol/e.fol_p)*e.fol_w as decimal(10,2)) desc
) as comprehensive_rank,


((a.pcu-nvl(c.pcu,0))/f.pcu_p)*f.pcu_w+
((a.weight-nvl(c.weight,0))/f.weight_p)*f.weight_w+
((a.fol-nvl(c.followers,0))/f.fol_p)*f.fol_w as grow_score,--总分

row_number()over(
partition by b.id
order by
--
cast(((a.pcu-nvl(c.pcu,0))/f.pcu_p)*f.pcu_w+
((a.weight-nvl(c.weight,0))/f.weight_p)*f.weight_w+
((a.fol-nvl(c.followers,0))/f.fol_p)*f.fol_w as decimal(10,2)) desc
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
(a.fol/e.fol_p)*e.fol_w as decimal(10,2)) desc
--
),--总分排序

------

b.id,--平台id
a.plat,--平台名称
a.rid,--主播id
a.name,--主播名字
a.pcu,--pcu
a.livetime,--开播时长


a.fol,--订阅
a.weight,--体重

cast((a.pcu/e.pcu_p)*e.pcu_w as decimal(10,2)),--pcu分数
cast((1-a.livetime/e.livetime_p)*e.livetime_w as decimal(10,2)),--开播时长分数

cast((a.weight/e.weight_p)*e.weight_w as decimal(10,2)),--体重增量分数
cast((a.fol/e.fol_p)*e.fol_w as decimal(10,2)),--订阅增量分数


--总分
cast((a.pcu/e.pcu_p)*e.pcu_w+
(1-a.livetime/e.livetime_p)*e.livetime_w+
(a.weight/e.weight_p)*e.weight_w+
(a.fol/e.fol_p)*e.fol_w as decimal(10,2)),

row_number()over(
partition by b.id
order by
--
cast((a.pcu/e.pcu_p)*e.pcu_w+
(1-a.livetime/e.livetime_p)*e.livetime_w+
(a.weight/e.weight_p)*e.weight_w+
(a.fol/e.fol_p)*e.fol_w as decimal(10,2)) desc
--
) -nvl(g.comprehensive_rank,0), --名次变化
a.room_content,

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

--成长中间表
insert overwrite table panda_competitor_result.anchor_m_grow_rank partition(par_date)
select 

case when d.rid is null then c.rid else d.rid end,
case when d.plat is null then c.plat else d.plat end,
case when d.category is null then c.category else d.category end,


case when d.grow_score is null then c.grow_score else d.grow_score end,
case when d.grow_rank is null then c.grow_rank else d.grow_rank end,

'${date}'
 
from (
select 
a.plat,a.rid,a.category,
cast(
((a.pcu-nvl(b.new_pcu,0))/f.pcu_p)*f.pcu_w+
(a.weight_changed/f.weight_p)*f.weight_w+
(a.followers_changed/f.fol_p)*f.fol_w as decimal(10,2)) as grow_score,--总分

row_number()over(
partition by a.plat
order by
--
cast(
((a.pcu-nvl(b.new_pcu,0))/f.pcu_p)*f.pcu_w+
(a.weight_changed/f.weight_p)*f.weight_w+
(a.followers_changed/f.fol_p)*f.fol_w as decimal(10,2)) desc
) grow_rank,

a.par_date
from panda_competitor_result.anchor_day_changed_analyse_by_sameday a
inner join(
select * from panda_competitor.crawler_all_anchor_analyse where par_date='${date_sub}'
) b on a.plat=b.plat and a.rid=b.rid and a.category=b.category
left join
(select * from panda_competitor_result.anchor_live_parameter where type=1) f
where a.par_date='${date}' 
) d
full outer join
(select * from panda_competitor_result.anchor_m_grow_rank where par_date='${date_sub}') c
on d.plat=c.plat and d.rid=c.rid and d.category=c.category;
"


hive -e "

--主播成长排名

insert overwrite table panda_competitor_result.anchor_growth_rank partition(par_date)


select 
row_number()over(
partition by a.plat
order by
--
round(
((a.pcu-nvl(b.new_pcu,0))/f.pcu_p)*f.pcu_w+
(a.weight_changed/f.weight_p)*f.weight_w+
(a.followers_changed/f.fol_p)*f.fol_w ,2) desc
) grow_rank,--排名
c.id,--平台id
a.plat,--平台名称
a.rid,--主播id
a.name,--主播名称
(a.pcu-nvl(b.new_pcu,0)),--pcu增值
a.followers_changed,--订阅增值
a.weight_changed,--体重增值
round(cast(((a.pcu-nvl(b.new_pcu,0))/f.pcu_p)*f.pcu_w as decimal(10,2)),2),--pcu增值分数

case when a.pcu=(a.pcu-nvl(b.new_pcu,0)) then 1 else 0 end,--pcu增值状态
round((a.weight_changed/f.weight_p)*f.weight_w,2),--体重增量分数
round((a.followers_changed/f.fol_p)*f.fol_w,2),--订阅增量分数
round(
((a.pcu-nvl(b.new_pcu,0))/f.pcu_p)*f.pcu_w+
(a.weight_changed/f.weight_p)*f.weight_w+
(a.followers_changed/f.fol_p)*f.fol_w,2) as grow_score,--总分

row_number()over(
partition by a.plat
order by
--
round(
((a.pcu-nvl(b.new_pcu,0))/f.pcu_p)*f.pcu_w+
(a.weight_changed/f.weight_p)*f.weight_w+
(a.followers_changed/f.fol_p)*f.fol_w ,2) desc
)-nvl(e.grow_rank,0),
g.title,
a.category,
'${date}'



from panda_competitor_result.anchor_day_changed_analyse_by_sameday a
inner join(
select * from panda_competitor.crawler_all_anchor_analyse where par_date='${date_sub}'
) b on a.plat=b.plat and a.rid=b.rid and a.category=b.category
left join panda_competitor.crawler_plat c ON a.plat = c.name
left join
(select * from panda_competitor_result.anchor_live_parameter where type=1) f

left join
(select * from panda_competitor_result.anchor_m_grow_rank where par_date='${date_sub}') e
on a.plat=e.plat and a.rid=e.rid and a.category=e.category
left join
(select * from panda_competitor.crawler_distinct_anchor where par_date='${date}') g
on a.plat=g.plat and a.rid=g.rid and a.category=g.category
where a.par_date='${date}' ;

"

#####生成excel
#综合
/usr/local/jdk1.8.0_60/bin/java -jar /home/likaiqing/hive-tool/export2excel.jar $date panda_competitor_result.anchor_comprehensive_rank par_date,rank,plat_id,plat,rid,name,puu,livetime,fol_up,weight_up,pcu_score,livetime_score,weight_score,fol_score,total_score,rank_change,room_content "日期,排序,平台ID,平台名称,主播ID,主播名称,PCU,开播时长,订阅,体重,PCU分数,开播时长分数,体重增量分数,订阅增量分数,总分,名次变化,房间内容" /data/tmp/zhengbo/file/
#成长
/usr/local/jdk1.8.0_60/bin/java -jar /home/likaiqing/hive-tool/export2excel.jar $date panda_competitor_result.anchor_growth_rank par_date,rank,plat_id,plat,rid,name,pcu_up,fol_up,weight_up,pcu_score,pcu_type,weight_up_score,fol_up_score,total_score,rank_change,room_content,category "日期,排序,平台ID,平台名称,主播ID,主播名称,PCU增值,订阅增量,体重增量,PCU增值分数,PCU增值状态,体重增量分数,订阅增量分数,总分,名次变化,房间内容,版区" /data/tmp/zhengbo/file/


#发送邮件
#
#/usr/local/jdk1.8.0_60/bin/java -jar /home/likaiqing/hive-tool/send_mail.jar "主播综合" "内容见附件" /data/tmp/zhengbo/file/anchor_comprehensive_rank${date}.xlsx "zhengbo@panda.tv" "baimuhai@panda.tv,lushenggang@panda.tv,wangshuo@panda.tv,likaiqing@panda.tv,zhaolirong@panda.tv,fengwenbo@panda.tv"

#
#/usr/local/jdk1.8.0_60/bin/java -jar /home/likaiqing/hive-tool/send_mail.jar "主播成长" "内容见附件" /data/tmp/zhengbo/file/anchor_growth_rank${date}.xlsx "zhengbo@panda.tv" "baimuhai@panda.tv,lushenggang@panda.tv,wangshuo@panda.tv,likaiqing@panda.tv,zhaolirong@panda.tv,fengwenbo@panda.tv"

####################
#20170123新增      平台，版区




hive -e "
insert overwrite table panda_competitor_result.plat_category_parameter partition(par_date)
--平台
select 43000,
1000000,
6500,
20,40,40,0,'${date}' as par_date
from default.dual
union all
--版区
select 9000,
600000,
1200,
20,40,40,1,'${date}' as par_date
from default.dual;
"

#######平台
hive -e "
insert overwrite table panda_competitor_result.plat_comprehensive_rank partition(par_date)
select 
row_number()over(
partition by a.par_date
order by
 --
cast(((a.followers_changed/d.fol)*d.fol_w+
(c.valid_anchor/d.anchor)*d.anchor_w+
(c.sum_anchor/d.live)*d.live_w
) as decimal(10,2)) desc
--
 ),--排序

b.id,--平台ID
a.plat,--平台名称
a.followers_changed,--总订阅增量
c.valid_anchor,--有效主播数
c.sum_anchor,--开播数
cast((a.followers_changed/d.fol)*d.fol_w as decimal(10,2)),--总订阅增量分数
cast((c.valid_anchor/d.anchor)*d.anchor_w as decimal(10,2)),--有效主播数分数
cast((c.sum_anchor/d.live)*d.live_w as decimal(10,2)),--开播数分数

cast(((a.followers_changed/d.fol)*d.fol_w+
(c.valid_anchor/d.anchor)*d.anchor_w+
(c.sum_anchor/d.live)*d.live_w
) as decimal(10,2)),--总分
a.par_date
from panda_competitor_result.plat_day_changed_analyse_by_sameday a
INNER JOIN panda_competitor.crawler_plat b ON a.plat = b.name
left join
(
select plat,
count(distinct case when pcu>1050 then rid else null end) as valid_anchor,
count(distinct rid) sum_anchor
from panda_competitor_result.anchor_day_changed_analyse_by_sameday 
where par_date='${date}'
group by plat
) c on a.plat=c.plat

left join
(
select *
from panda_competitor_result.plat_category_parameter
where par_date='${date}'
and type=0
) d

where a.par_date='${date}';
"

####版区
hive -e "
insert overwrite table panda_competitor_result.category_comprehensive_rank partition(par_date)
select 
row_number()over(
partition by b.id
order by
 --
cast(((a.followers_changed/d.fol)*d.fol_w+
(c.valid_anchor/d.anchor)*d.anchor_w+
(c.sum_anchor/d.live)*d.live_w
) as decimal(10,2)) desc
--
 ),--排序
 

b.id,--平台ID
a.plat,--平台名称
a.category,--版区名称
c.sum_anchor,--开播数
a.followers_changed,--总订阅增量
c.valid_anchor,--有效主播数

cast((c.sum_anchor/d.live)*d.live_w as decimal(10,2)),--开播数分数
cast((a.followers_changed/d.fol)*d.fol_w as decimal(10,2)),--总订阅增量分数
cast((c.valid_anchor/d.anchor)*d.anchor_w as decimal(10,2)),--有效主播数分数

cast(((a.followers_changed/d.fol)*d.fol_w+
(c.valid_anchor/d.anchor)*d.anchor_w+
(c.sum_anchor/d.live)*d.live_w
) as decimal(10,2)),--总分
a.par_date
from panda_competitor_result.category_day_changed_analyse_by_sameday a
INNER JOIN panda_competitor.crawler_plat b ON a.plat = b.name
left join
(
select plat,category,
count(distinct case when pcu>1050 then rid else null end) as valid_anchor,
count(distinct rid) sum_anchor
from panda_competitor_result.anchor_day_changed_analyse_by_sameday 
where par_date='${date}' 
group by plat,category
) c on a.plat=c.plat and a.category=c.category
left join(
select *
from panda_competitor_result.plat_category_parameter
where par_date='${date}'
and type=1
)d
where a.par_date='${date}';
"
#/home/likaiqing/hive-tool/export2excel.jar

#####生成excel
#平台
/usr/local/jdk1.8.0_60/bin/java -jar /home/likaiqing/hive-tool/export2excel.jar ${date} panda_competitor_result.plat_comprehensive_rank par_date,rank,plat_id,plat,fol_grow,v_anchor,s_anchor,fol_score,v_ahchor_score,s_anchor_score,total "日期,排序,平台ID,平台名称,总订阅增量,有效主播数,开播数,总订阅增量分数,有效主播数分数,开播数分数,总分" /data/tmp/zhengbo/file/

#版区
/usr/local/jdk1.8.0_60/bin/java -jar /home/likaiqing/hive-tool/export2excel.jar ${date} panda_competitor_result.category_comprehensive_rank par_date,rank,plat_id,plat,category,s_anchor,fol_grow,v_anchor,s_anchor_score,fol_score,v_ahchor_score,total "日期,排序,平台ID,平台名称,版区名称,开播数,总订阅增量,有效主播数,开播数分数,总订阅增量分数,有效主播数分数,总分" /data/tmp/zhengbo/file/
 

#发送邮件
#平台
#/usr/local/jdk1.8.0_60/bin/java -jar /home/likaiqing/hive-tool/send_mail.jar "平台综合" "内容见附件" /data/tmp/zhengbo/file/plat_comprehensive_rank${date}.xlsx "zhengbo@panda.tv" "baimuhai@panda.tv,lushenggang@panda.tv,wangshuo@panda.tv,likaiqing@panda.tv,zhaolirong@panda.tv,fengwenbo@panda.tv"

#版区
#/usr/local/jdk1.8.0_60/bin/java -jar /home/likaiqing/hive-tool/send_mail.jar "版区综合" "内容见附件" /data/tmp/zhengbo/file/category_comprehensive_rank${date}.xlsx "zhengbo@panda.tv" "baimuhai@panda.tv,lushenggang@panda.tv,wangshuo@panda.tv,likaiqing@panda.tv,zhaolirong@panda.tv,fengwenbo@panda.tv"

#压缩
zip -m /data/tmp/zhengbo/file/rank_${date} /data/tmp/zhengbo/file/anchor_comprehensive_rank${date}.xlsx /data/tmp/zhengbo/file/anchor_growth_rank${date}.xlsx /data/tmp/zhengbo/file/plat_comprehensive_rank${date}.xlsx /data/tmp/zhengbo/file/category_comprehensive_rank${date}.xlsx

###发送邮件
/usr/local/jdk1.8.0_60/bin/java -jar /home/likaiqing/hive-tool/send_mail.jar "排名相关" "内容见附件" /data/tmp/zhengbo/file/rank_${date}.zip "zhengbo@panda.tv" "baimuhai@panda.tv,lushenggang@panda.tv,wangshuo@panda.tv,likaiqing@panda.tv,zhaolirong@panda.tv,fengwenbo@panda.tv"




##########################################################################################################################
##2017-02-21 推荐位修改


#推荐位日表
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
  round(a.rectimes*3/60,2) ,
  e.url,
  e.category,
  e.title,
  f.total_score,
  g.total_score,
  a.par_date
FROM panda_competitor_result.crawler_anchor_day a
  INNER JOIN panda_competitor.crawler_plat b ON a.plat = b.name
  INNER JOIN panda_competitor_result.crawler_anchor_change_day d
    ON d.par_date = '${date}' AND a.plat = d.plat AND a.rid = d.rid
  LEFT JOIN panda_competitor.crawler_distinct_anchor e
    ON e.par_date = '${date}' AND a.rid = e.rid AND a.plat = e.plat AND a.category = e.category
  left join 
  (select plat_id,rid,total_score from panda_competitor_result.anchor_comprehensive_rank where par_date='${date}') f on b.id=f.plat_id and a.rid=f.rid
  left join
  (select plat_id,rid,total_score from panda_competitor_result.anchor_growth_rank where par_date='${date}') g on b.id=g.plat_id and a.rid=g.rid
WHERE a.par_date = '${date}'
      AND a.rectimes > 0;
"
