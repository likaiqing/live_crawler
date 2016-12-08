#!/bin/bash

for d in $(seq -w 24 28)
do
	sh /home/likaiqing/shell/crawler_ana/crawler_report.sh 201611$d
done
