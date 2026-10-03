# Pinned versions: Spark 4.0.4 (Scala 2.13, Java 17, Python 3.10), Delta Lake 4.0.1, Hadoop 3.4.1 (hadoop-aws),
# AWS SDK v2 bundle 2.24.6, Kafka clients 3.9.1, commons-pool2 2.12.0
FROM apache/spark:4.0.4-scala2.13-java17-python3-ubuntu@sha256:94ad730f7510002d8a1615de269f27cdeca4d4eef51657384db3fa9246b5a4d8

ARG SPARK_VERSION=4.0.4
ARG DELTA_VERSION=4.0.1
ARG HADOOP_VERSION=3.4.1
ARG AWS_BUNDLE_VERSION=2.24.6
ARG KAFKA_CLIENTS_VERSION=3.9.1
ARG COMMONS_POOL2_VERSION=2.12.0
ARG MAVEN=https://repo1.maven.org/maven2

USER root
RUN set -eu; cd "$SPARK_HOME/jars"; \
    ls | grep -q "hadoop-client-api-${HADOOP_VERSION}.jar"; \
    for jar in \
      "io/delta/delta-spark_2.13/${DELTA_VERSION}/delta-spark_2.13-${DELTA_VERSION}.jar" \
      "io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar" \
      "org/apache/spark/spark-sql-kafka-0-10_2.13/${SPARK_VERSION}/spark-sql-kafka-0-10_2.13-${SPARK_VERSION}.jar" \
      "org/apache/spark/spark-token-provider-kafka-0-10_2.13/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.13-${SPARK_VERSION}.jar" \
      "org/apache/spark/spark-avro_2.13/${SPARK_VERSION}/spark-avro_2.13-${SPARK_VERSION}.jar" \
      "org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
      "org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar" \
      "org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar"; \
    do curl -fsSLO --retry 8 --retry-all-errors --retry-delay 3 -C - "${MAVEN}/${jar}"; done

RUN curl -fsSL --retry 8 --retry-all-errors --retry-delay 3 -C - -o "$SPARK_HOME/jars/bundle-${AWS_BUNDLE_VERSION}.jar" \
    "${MAVEN}/software/amazon/awssdk/bundle/${AWS_BUNDLE_VERSION}/bundle-${AWS_BUNDLE_VERSION}.jar"

RUN pip install --no-cache-dir "pyspark==${SPARK_VERSION}" "delta-spark==${DELTA_VERSION}" pytest fastavro

COPY spark-defaults.conf /opt/spark-defaults.conf.template
COPY entrypoint.sh /opt/lakehouse-entrypoint.sh
RUN chmod +x /opt/lakehouse-entrypoint.sh && mkdir -p "$SPARK_HOME/conf" && chown -R spark:spark "$SPARK_HOME/conf"

RUN ln -s /usr/bin/python3 /usr/local/bin/python
USER spark
WORKDIR /opt/lakehouse
ENTRYPOINT ["/opt/lakehouse-entrypoint.sh"]
