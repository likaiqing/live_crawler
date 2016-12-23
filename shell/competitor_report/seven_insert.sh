#!/bin/bash

date=$1
date=${date:=`date -d 'yesterday' +%Y%m%d`}
date_sub=`date -d "-1day $date" +%Y%m%d`

#主播PCU排行
hive -e "
insert overwrite table panda_competitor_result.anchor_pcu_rank partition(par_date)
select b.id,a.plat,a.rid,a.name,a.pcu_plat_rank,a.pcu,a.weight,a.fol,
nvl(cast(a.pcu_plat_rank-c.new_pcu_plat_rank as int),'新上榜'),
a.category,a.par_date
from panda_competitor_result.crawler_anchor_day a
inner join panda_competitor.crawler_plat b on a.plat=b.name
left join panda_competitor.crawler_all_anchor_analyse c
on c.par_date='${date_sub}' and a.plat=c.plat and a.rid=c.rid and a.category=c.category
where a.par_date='${date}'
and a.pcu_plat_rank>=1 and a.pcu_plat_rank<=1000;"


#主播变化趋势增减表
hive -e "
insert overwrite table panda_competitor_result.anchor_changed_rank partition(par_date)
select b.id,a.plat,a.rid,a.name,a.category,
a.pcu,
nvl(cast(a.pcu_plat_rank-c.new_pcu_plat_rank as int),'新上榜'),
a.livetime,
nvl(cast(a.livetime_plat_rank-c.new_duration_plat_rank as int),'新上榜'),
a.weight,
nvl(cast(a.weight_plat_rank-c.new_weight_plat_rank as int),'新上榜'),
a.par_date

from panda_competitor_result.crawler_anchor_day a
inner join panda_competitor.crawler_plat b on a.plat=b.name
left join panda_competitor.crawler_all_anchor_analyse c
on c.par_date='${date_sub}' and a.plat=c.plat and a.rid=c.rid and a.category=c.category
where a.par_date='${date}'
and a.pcu_plat_rank>=1 and a.pcu_plat_rank<=1000;"


#主播订阅排行榜
hive -e "
insert overwrite table panda_competitor_result.anchor_fol_rank partition(par_date)
select
b.id,
a.plat,
a.rid,
a.name,
a.fol_plat_rank,
a.pcu,
a.weight,
a.fol,
nvl(cast(a.fol-c.new_followers as int),'新上榜'),
a.category,
a.par_date

from panda_competitor_result.crawler_anchor_day a
inner join panda_competitor.crawler_plat b on a.plat=b.name
left join panda_competitor.crawler_all_anchor_analyse c
on c.par_date='${date_sub}' and a.plat=c.plat and a.rid=c.rid and a.category=c.category
where a.par_date='${date}'
and a.fol_plat_rank>=1 and a.fol_plat_rank<=1000;"

#主播订阅增长排行表
hive -e "
insert overwrite table panda_competitor_result.anchor_fol_up_rank partition(par_date)
select b.id,
a.plat,
a.rid,
a.name,
d.fol_changed_plat_rank,
a.pcu,
a.weight,
a.fol,
d.fol_changed,
a.category,
a.par_date


from panda_competitor_result.crawler_anchor_day a
inner join panda_competitor.crawler_plat b on a.plat=b.name
left join panda_competitor.crawler_all_anchor_analyse c
on c.par_date='${date_sub}' and a.plat=c.plat and a.rid=c.rid and a.category=c.category
inner join panda_competitor_result.crawler_anchor_change_day d
on d.par_date='${date}' and a.plat=d.plat and a.rid=d.rid
where a.par_date='${date}'
and d.fol_changed_plat_rank>=1 and d.fol_changed_plat_rank<=1000;"


#主播体重增长排行表
hive -e "
insert overwrite table panda_competitor_result.anchor_weight_rank partition(par_date)
select b.id,
a.plat,
a.rid,
a.name,
a.weight_plat_rank,
a.pcu,
a.fol,
c.new_weight,
a.weight,
a.category,
a.par_date

from panda_competitor_result.crawler_anchor_day a
inner join panda_competitor.crawler_plat b on a.plat=b.name
left join panda_competitor.crawler_all_anchor_analyse c
on c.par_date='${date_sub}' and a.plat=c.plat and a.rid=c.rid and a.category=c.category
where a.par_date='${date}'
and a.weight_plat_rank>=1 and a.weight_plat_rank<=1000;"


#主播播放时长排行表
hive -e "
insert overwrite table panda_competitor_result.anchor_livetime_rank partition(par_date)
select b.id,
a.plat,
a.rid,
a.name,
a.livetime_plat_rank,
a.pcu,
a.fol,
c.new_duration_plat_rank,
a.category,
a.par_date
from panda_competitor_result.crawler_anchor_day a
inner join panda_competitor.crawler_plat b on a.plat=b.name
left join panda_competitor.crawler_all_anchor_analyse c
on c.par_date='${date_sub}' and a.plat=c.plat and a.rid=c.rid and a.category=c.category
where a.par_date='${date}'
and a.livetime_plat_rank>=1 and a.livetime_plat_rank<=1000;"


#推荐味日表
hive -e "
insert overwrite table panda_competitor_result.indexrec_day_report partition(par_date)
select 
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
from panda_competitor_result.crawler_anchor_day a
inner join panda_competitor.crawler_plat b on a.plat=b.name
inner join panda_competitor_result.crawler_anchor_change_day d
on d.par_date='${date}' and a.plat=d.plat and a.rid=d.rid
left join panda_competitor.crawler_distinct_anchor e
on e.par_date='${date}' and a.rid=e.rid and a.plat=e.plat and a.category=e.category
where a.par_date='${date}'
and a.rectimes>0;"