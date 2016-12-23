#!/bin/bash
date=${1}
date_sub=1
if [[ ${date} == "" ]]
then

date=`date -d "-1 day" +%Y%m%d`;
date_sub=`date -d "-2 day"+%Y%m%d`

fi
tmp=$(date +%s -d ${date} );
tmp=`expr ${tmp} - 86400`;
date_sub=`date -d @${tmp} "+%Y%m%d"`;

echo ${date};
echo ${date_sub};

#主播日数据
hive -e "
insert overwrite table panda_competitor_result.crawler_anchor_day partition(par_date)
select 
plat,
category,
rid,
name,--主播昵称
pcu,
row_number()over(partition by plat order by pcu desc),
row_number()over(partition by plat,category order by pcu desc),
duration,
row_number()over(partition by plat order by duration desc),
row_number()over(partition by plat,category order by duration desc),
rec_times,
row_number()over(partition by plat order by rec_times desc),
row_number()over(partition by plat,category order by rec_times desc) ,
followers,
row_number()over(partition by plat order by followers desc),
row_number()over(partition by plat,category order by followers desc),
weight,
row_number()over(partition by plat order by weight desc),
row_number()over(partition by plat,category order by weight desc),
null,--gift_users	赠送礼物人数
null,---gift_times	赠送礼物次数
null,--gift_value	赠送礼物价值
null,--giftvalue_plat_rank	平台礼物价值排名
null,--giftvalue_cate_rank	版区礼物价值排名
null,--barrage_users	发弹幕人数
null,--barrage_amount	发弹幕数量
null,--barrage_plat_rank	平台弹幕数排名
null,--barrage_cate_rank	版区弹幕数排名
is_new,	--是否是新主播
par_date
from(
select a.par_date,
a.plat,
c.category,
a.rid,
b.name,
min(a.is_new) as is_new,
sum(a.pcu) as pcu,
sum(a.duration) as duration,
max(a.rec_times) as rec_times,
max(a.followers) as followers,
max(a.weight) as weight
from panda_competitor.crawler_day_anchor_analyse a
left join
(select rid,plat,name,count(*) as ff from panda_competitor.crawler_distinct_anchor where par_date='${date}' group by rid,plat,name) b
on a.rid =b.rid and a.plat=b.plat
left join
(select plat,rid,category,new_pcu,row_number()over(partition by plat,rid order by  new_pcu desc) as rw from panda_competitor.crawler_day_anchor_analyse where par_date='${date}')c
on a.plat=c.plat and a.rid=c.rid and c.rw=1
where a.par_date='${date}' 
group by a.par_date,a.plat,c.category,a.rid,b.name
) zz;
"


hive -e "
insert overwrite table panda_competitor_result.crawler_anchor_change_day partition(par_date)
select aa.plat,--	平台
aa.category,--	版区
aa.rid,--主播ID
aa.name,--主播昵称
(aa.pcu-nvl(bb.pcu,0)), --pcu_changed	PCU变化量
row_number()over(partition by aa.plat order by (aa.pcu-nvl(bb.pcu,0)) desc),--pcu_changed_plat_rank	
row_number()over(partition by aa.plat,aa.category order by (aa.pcu-nvl(bb.pcu,0)) desc),--pcu_changed_cate_rank	
(aa.duration-nvl(bb.duration,0)),--livetime_changed	直播时长变化量
row_number()over(partition by aa.plat order by (aa.duration-nvl(bb.duration,0)) desc),--livetime_changed_plat_rank	
row_number()over(partition by aa.plat,aa.category order by (aa.duration-nvl(bb.duration,0)) desc),--livetime_changed_cate_rank
(aa.rec_times-nvl(bb.rec_times,0)),--rec_changed	推荐次数变化量
row_number()over(partition by aa.plat order by (aa.rec_times-nvl(bb.rec_times,0)) desc),--rec_changed_plat_rank	
row_number()over(partition by aa.plat,aa.category order by (aa.rec_times-nvl(bb.rec_times,0)) desc) ,--rec_changed_cate_rank	
(aa.followers-nvl(bb.followers,0)),--fol_changed	关注变化量
row_number()over(partition by aa.plat order by (aa.followers-nvl(bb.followers,0)) desc),--fol_changed_plat_rank	
row_number()over(partition by aa.plat,aa.category order by (aa.followers-nvl(bb.followers,0)) desc),--fol_changed_cate_rank	
(aa.weight-nvl(bb.weight,0)),--weight_changed	体重变化量
row_number()over(partition by aa.plat order by (aa.weight-nvl(bb.weight,0)) desc),--weight_changed_plat_rank	
row_number()over(partition by aa.plat,aa.category order by (aa.weight-nvl(bb.weight,0)) desc),--weight_changed_cate_rank	
null,--giftvalue_changed	礼物价值变化量
null,--giftvalue_changed_plat_rank	
null,--giftvalue_changed_cate_rank	
null,--barrage_changed	弹幕数变化量
null,--barrage_changed_plat_rank	
null,--barrage_changed_cate_rank	
aa.par_date
from

(select a.par_date,
a.plat,
c.category,
a.rid,
b.name,
sum(a.pcu) as pcu,
sum(a.duration) as duration,
max(a.rec_times) as rec_times,
max(a.followers) as followers,
max(a.weight) as weight
from panda_competitor.crawler_day_anchor_analyse a
left join
(select rid,plat,name,count(*) as ff from panda_competitor.crawler_distinct_anchor where par_date='${date}' group by rid,plat,name) b
on a.rid =b.rid and a.plat=b.plat
left join
(select plat,rid,category,new_pcu,row_number()over(partition by plat,rid order by  new_pcu desc) as rw from panda_competitor.crawler_day_anchor_analyse where par_date='${date}')c
on a.plat=c.plat and a.rid=c.rid and c.rw=1
where par_date='${date}' 
group by a.par_date,a.plat,c.category,a.rid,b.name) aa
left join 
(
select a.par_date,
a.plat,
a.rid,
sum(new_pcu) as pcu,
sum(new_duration) as duration,
max(new_rec_times) as rec_times,
max(new_followers) as followers,
max(new_weight) as weight
from panda_competitor.crawler_all_anchor_analyse a
where par_date='${date_sub}'
group by par_date,a.plat,a.rid
) bb
on aa.plat=bb.plat  and aa.rid=bb.rid;
"


