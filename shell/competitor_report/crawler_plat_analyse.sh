#!/bin/bash

date=$1
date=${date:=`date -d 'yesterday' +%Y%m%d`}
sub_1_days=`date -d "-1day $date" +%Y%m%d`


hive -e "
insert overwrite table panda_competitor_result.plat_day_changed partition(par_date)

"

hive -e "
insert overwrite table panda_competitor_result.plat_day_change_analyse partition(par_date)

"