#!/bin/bash

origin_dir=~/space/panda/live_crawler/shell
remote_dir=/home/likaiqing/shell

for sub_dir in crawler crawler_ana crawler_detail_ana competitor_report competitor_changed_sameday
do
    rsync -auvz $origin_dir/$sub_dir/ 10.110.16.33:$remote_dir/$sub_dir/
done

for sub_dir in competitor_shell
do
    rsync -auvz $origin_dir/pd77/$sub_dir/ 10.110.20.77:$remote_dir/crawler_ana/$sub_dir/
done