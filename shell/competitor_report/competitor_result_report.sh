#!/bin/bash

date=$1
date=${date:=`date -d 'yesterday' +%Y%m%d`}


hive -e "
insert overwrite table panda_competitor_result.category_day_report partition(par_date)

"
