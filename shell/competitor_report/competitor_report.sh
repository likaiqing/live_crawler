#!/bin/bash

date=$1
date=${date:=`date -d 'yesterday' +%Y%m%d`}

cd `dirname $0`
sh $(dirname $0)/../crawler_ana/crawler_distinct_anchor.sh $date
echo "crawler_distinct_anchor.sh $date down"
sh $(dirname $0)/../crawler_ana/crawler_distinct_category.sh $date
echo "crawler_distinct_category.sh $date down"
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
sh $(dirname $0)/competitor_result_analyse.sh $date
echo "competitor_result_analyse.sh $date down"
sh $(dirname $0)/anchor_change_day.sh $date
echo "anchor_change_day.sh $date down"
sh $(dirname $0)/../competitor_changed_sameday/anchor_change_sameday.sh $date
echo "anchor_change_sameday.sh $date down"
sh $(dirname $0)/../competitor_changed_sameday/category_change_sameday.sh $date
echo "category_change_sameday.sh $date down"
sh $(dirname $0)/../competitor_changed_sameday/plat_change_sameday.sh $date
echo "plat_change_sameday.sh $date down"
sh $(dirname $0)/seven_insert.sh $date
echo "seven_insert.sh $date down"

ssh 10.110.20.77 "sh /home/likaiqing/shell/crawler_ana/competitor_shell/com_result_export2excel.sh $date"