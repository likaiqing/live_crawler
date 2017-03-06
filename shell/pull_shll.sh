#!/bin/bash

origin_dir=~/space/panda/live_crawler/shell
remote_dir=/home/likaiqing/shell


for sub_dir in crawler crawler_ana crawler_detail_ana competitor_report
do
    rsync -auvz 10.110.16.33:$remote_dir/$sub_dir/*.sh $origin_dir/$sub_dir/
done

for sub_dir in crawler_ana/competitor_shell crawler
do
    rsync -auvz 10.110.20.77:$remote_dir/$sub_dir/*.sh $origin_dir/pd77/$sub_dir/
done