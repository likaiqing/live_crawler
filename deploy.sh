#!/bin/bash

jar=$1
jar=${jar:=live_crawler}
mvn clean compile package -Dmaven.test.skip=true
mv target/live_crawler-1.0-SNAPSHOT-jar-with-dependencies.jar target/${jar}.jar

for ip in 180.97.220.220
do
	scp -p target/${jar}.jar root@$ip:/root/jar/
done