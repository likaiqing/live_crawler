#!/bin/bash

jar=/home/likaiqing/hive-tool/douyuanchor2file.jar
task=douyuanchor2file
dest=/home/likaiqing/shell/douyu_anchors/conf.properties
/usr/local/jdk1.8.0_60/bin/java -jar $jar $task $dest


