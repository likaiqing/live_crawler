#!/bin/bash

jar=$1
jar=${jar:=live_crawler}
mvn clean compile package -Dmaven.test.skip=true
mv target/live_crawler-1.0-SNAPSHOT-jar-with-dependencies.jar target/${jar}.jar

for ip in 222.186.169.41
do
	scp -p target/${jar}.jar root@$ip:/root/jar/
done