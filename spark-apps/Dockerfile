FROM apache/spark:3.5.7-python3

USER root
# Install system dependencies
RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

# ensure Spark standard jars folder exists
# RUN mkdir -p /opt/spark/jars && chown -R spark:spark /opt/spark/jars

# copy app code
RUN mkdir -p /opt/spark-apps && chown -R spark:spark /opt/spark-apps
COPY . /opt/spark-apps
RUN chown -R spark:spark /opt/spark-apps

# download required Kafka + Mongo JARs + JMX Exporters
RUN set -eux; \
    wget -O /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    wget -O /opt/spark/jars/spark-streaming-kafka-0-10-assembly_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10-assembly_2.12/3.5.0/spark-streaming-kafka-0-10-assembly_2.12-3.5.0.jar && \
    wget -O /opt/spark/jars/kafka-clients-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar && \
    wget -O /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    wget -O /opt/spark/jars/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    wget -O /opt/spark/jars/mongo-spark-connector_2.12-10.3.0.jar \
    https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.3.0/mongo-spark-connector_2.12-10.3.0.jar && \
    wget -O /opt/spark/jars/mongodb-driver-sync-4.8.0.jar \
    https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/4.8.0/mongodb-driver-sync-4.8.0.jar && \
    wget -O /opt/spark/jars/mongodb-driver-core-4.8.0.jar \
    https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/4.8.0/mongodb-driver-core-4.8.0.jar && \
    wget -O /opt/spark/jars/bson-4.8.0.jar \
    https://repo1.maven.org/maven2/org/mongodb/bson/4.8.0/bson-4.8.0.jar && \
    wget -O /opt/spark/jars/jmx_prometheus_javaagent-0.20.0.jar \
    https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.20.0/jmx_prometheus_javaagent-0.20.0.jar && \
    echo "✅ All JARs placed into /opt/spark/jars"

USER spark
WORKDIR /opt/spark-apps

CMD ["/opt/spark-apps/start-spark.sh"]

