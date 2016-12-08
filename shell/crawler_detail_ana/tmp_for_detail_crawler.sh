#!/bin/bash

for d in $(seq -w 1 4)
do 
	sh /home/likaiqing/shell/crawler_detail_ana/tmp_crawler_detail.sh 2016120$d
done 
