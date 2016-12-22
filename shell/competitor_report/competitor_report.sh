#!/bin/bash

date=$1
date=${date:=`date -d 'yesterday' +%Y%m%d`}

cd `dirname $0`
sh $(dirname $0)/crawler_day_anchor_ana.sh $date
echo "crawler_day_anchor_ana.sh $date down"
sh $(dirname $0)/crawler_day_cate_ana.sh $date
echo "crawler_day_cate_ana.sh $date down"
sh $(dirname $0)/crawler_day_plat_ana.sh $date
echo "crawler_day_plat_ana.sh $date down"
sh $(dirname $0)/crawler_all_anchor_ana.sh $date
echo "crawler_all_anchor_ana.sh $date down"
sh $(dirname $0)/crawler_all_cate_ana.sh $date
echo "crawler_all_cate_ana.sh $date down"
sh $(dirname $0)/crawler_all_plat_ana.sh $date
echo "crawler_all_plat_ana.sh $date down"
sh $(dirname $0)/crawler_category_analyse.sh $date
echo "crawler_category_analyse.sh $date down"
sh $(dirname $0)/crawler_plat_analyse.sh $date
echo "crawler_plat_analyse.sh $date down"

#ssh 10.110.20.77 "sh /home/likaiqing/shell/crawler_ana/competitor_shell/com_result_export2excel.sh $date"