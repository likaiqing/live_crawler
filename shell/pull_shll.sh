#!/bin/bash

origin_dir=~/space/panda/live_crawler/shell
remote_dir=/home/likaiqing/shell

rsync -auvz $origin_dir/$sub_dir/ 10.110.16.33:$remote_dir/$sub_dir/
for sub_dir in crawler crawler_ana crawler_detail_ana
do
    scp -p 10.110.16.33:$remote_dir/$sub_dir/* $origin_dir/$sub_dir/
done