#!/bin/bash

date=$1
date=${date:=`date -d 'yesterday' +%Y%m%d`}

sh $(dirname $0)/../competitor_changed_sameday/anchor_change_sameday.sh $date
echo "anchor_change_sameday.sh $date down"
sh $(dirname $0)/../competitor_changed_sameday/category_change_sameday.sh $date
echo "category_change_sameday.sh $date down"
sh $(dirname $0)/../competitor_changed_sameday/plat_change_sameday.sh $date
echo "plat_change_sameday.sh $date down"

