#!/bin/sh
mvn package
cp target/rocketmq-console-3.2.2.R2.war ~/jetty/webapps/
cp doc/rocketmq-console-3.2.2.R2.xml ~/jetty/webapps/
