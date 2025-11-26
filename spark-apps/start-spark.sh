#!/bin/bash
set -e

# Запуск Spark Job (local[*] — использует все ядра контейнера)
echo "Starting Spark Streaming Job..."
/opt/spark/bin/spark-submit \
    --master local[*] \
    --jars $(echo /opt/spark/jars/*.jar | tr ' ' ',') \
    --conf "spark.driver.extraJavaOptions=-javaagent:${JMX_AGENT}=${JMX_PORT}:${JMX_CONFIG}" \
    --conf "spark.executor.extraJavaOptions=-javaagent:${JMX_AGENT}=${JMX_PORT}:${JMX_CONFIG}" \
    --conf "spark.mongodb.connection.uri=${MONGO_URI}" \
    /opt/spark-apps/stream_processing.py


