#!/bin/bash


#for d in $(seq -w 26 30)
#do
#    sh $(dirname $0)/competitor_report.sh 201611$d
#done
for d in $(seq -w 20 28)
do
    sh $(dirname $0)/crawler_day_cate_ana.sh 201612$d
    sh $(dirname $0)/crawler_day_plat_ana.sh 201612$d
    sh $(dirname $0)/crawler_all_cate_ana.sh 201612$d
    sh $(dirname $0)/crawler_all_plat_ana.sh 201612$d
    sh $(dirname $0)/crawler_category_analyse.sh 201612$d
    sh $(dirname $0)/crawler_plat_analyse.sh 201612$d
    sh $(dirname $0)/competitor_result_analyse.sh 201612$d
done
