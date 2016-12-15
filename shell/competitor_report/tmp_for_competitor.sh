#!/bin/bash

for d in $(seq -w 26 30)
do
    sh $(dirname $0)/competitor_report.sh 201611$d
done

for d in $(seq -w 01 14)
do
    sh $(dirname $0)/competitor_report.sh 201612$d
done