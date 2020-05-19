#!/bin/bash

HOME_PATH=$(cd "$(dirname "$0")"; cd ..; pwd)

ENV_PATH=${HOME_PATH}/config/env.sh

echo -e "====================== ENV_PATH = ${ENV_PATH}"

source "${ENV_PATH}" "${HOME_PATH}"

INPUT=$*
if [[ "x${INPUT}" = "x" ]]; then
    YEARSTERDAY_STR=$(date +"%Y-%m-%d" -d "-1 day")
else
    YEARSTERDAY_STR=${INPUT}
fi
echo "${YEARSTERDAY_STR}"
######################################################
##  UserBaseCF v1
######################################################
SPARK_CMD=$(cat <<!EOF
 spark-submit \
    --class com.winfred.datamining.kafka.SparkStreamingTest \
    --master ${SPARK_MASTER} \
    --deploy-mode ${SPARK_DEPLOY_MODE} \
    --queue ${SPARK_QUEUE_NAME} \
    --driver-memory 4g \
    --num-executors 4 \
    --executor-memory 4g \
    --executor-cores 4 \
    --conf spark.executor.extraJavaOptions=-Xss8m \
    --conf spark.driver.userClassPathFirst=true \
    --jars ${SPARK_DEPENDENCY_JARS} \
    --verbose \
  ${PATH_STREAMING_JAR} \
  --bootstrap-servers 172.27.0.39:9092 --topic-name kevinnhu-test-v1
!EOF
)
echo -e "==============================================================\n ${SPARK_CMD} \n=============================================================="

${SPARK_CMD}