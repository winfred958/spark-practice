#!/bin/bash

SPARK_HOME=/usr/local/service/spark

SPARK_DEPENDENCY_JARS=/usr/share/java/mysql-connector-java.jar
SPARK_CONFIG_FILES=/etc/spark/conf/hive-site.xml

HOME_PATH=${1}


PATH_MYSQL_CONNECTOR_JAR=/usr/share/java/mysql-connector-java.jar

SPARK_MASTER=yarn
SPARK_DEPLOY_MODE=cluster
SPARK_QUEUE_NAME=default

PATH_STREAMING_JAR="${HOME_PATH}"/lib/practice-streaming.jar
