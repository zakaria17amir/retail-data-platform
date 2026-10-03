# Pinned versions: Confluent cp-kafka-connect-base 8.2.4 (Kafka 4.2), Debezium PostgreSQL connector 3.7.0.Final
FROM confluentinc/cp-kafka-connect-base:8.2.4@sha256:d2c5948644e05e3f521836991fdd1e70badc366dedbc9ea5db29ca55f695ab52

ARG DEBEZIUM_VERSION=3.7.0.Final

USER root
RUN curl -fsSL "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_VERSION}/debezium-connector-postgres-${DEBEZIUM_VERSION}-plugin.tar.gz" \
    | python3 -c "import sys, tarfile; tarfile.open(fileobj=sys.stdin.buffer, mode='r|gz').extractall('/usr/share/confluent-hub-components')" \
 && ls /usr/share/java/kafka-serde-tools | grep -i avro
USER appuser
