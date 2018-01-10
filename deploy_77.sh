#!/bin/bash

jar=$1
jar=${jar:=douyuanchor2file}
mvn clean compile package -Dmaven.test.skip=true
mv target/live_crawler-1.0-SNAPSHOT-jar-with-dependencies.jar target/${jar}.jar

for ip in 10.110.20.77
do
	rsync -auvz --password-file=/Users/likaiqing/.panda_rsyncd.secrets target/${jar}.jar likaiqing@$ip::likaiqing/hive-tool/
done