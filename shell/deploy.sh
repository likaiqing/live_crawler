#!/bin/bash

origin_dir=~/space/panda/live_crawler/shell
remote_dir=~/shell

for sub_dir in crawler crawler_ana crawler_detail_ana
do
    rsync -auvz $origin_dir/$sub_dir/ $remote_dir/$sub_dir/
done