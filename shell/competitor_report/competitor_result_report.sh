#!/bin/bash

date=$1
date=${date:=`date -d 'yesterday' +%Y%m%d`}

cd `dirname $0`
sh $(dirname $0)/crawler_day_anchor_ana.sh $date
echo "crawler_day_anchor_ana.sh $date down"
sh $(dirname $0)/crawler_day_cate_ana.sh $date
echo "crawler_day_cate_ana.sh $date down"
sh $(dirname $0)/crawler_all_anchor_ana.sh $date
echo "crawler_all_anchor_ana.sh $date down"
sh $(dirname $0)/crawler_all_cate_ana.sh $date
echo "crawler_all_cate_ana.sh $date down"