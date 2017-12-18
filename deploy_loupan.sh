#!/bin/bash

mvn clean compile package -Dmaven.test.skip=true
mv target/live_crawler-1.0-SNAPSHOT-jar-with-dependencies.jar target/live_crawler.jar

for ip in 59.110.227.31
do
	scp -p target/live_crawler.jar $ip:~/jar/
done