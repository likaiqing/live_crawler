#!/bin/bash


#for d in $(seq -w 26 30)
#do
#    sh $(dirname $0)/competitor_report.sh 201611$d
#done
for d in $(seq -w 20 22)
do
    sh $(dirname $0)/competitor_report.sh 201612$d
done
