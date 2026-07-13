# EKS Flink image = the local Flink image + the benchmark fat-jar baked in.
# Build context is the repo root (see 03_build_push_images.sh).
ARG FLINK_VERSION=2.2.0
FROM flink:${FLINK_VERSION}-java17

ARG ICEBERG_VERSION=1.11.0
ENV LIB=/opt/flink/lib
USER root
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/
RUN set -eux; for url in \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-2.0/${ICEBERG_VERSION}/iceberg-flink-runtime-2.0-${ICEBERG_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/5.0.0-2.2/flink-connector-kafka-5.0.0-2.2.jar" \
      "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.0/kafka-clients-3.9.0.jar" \
      "https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar" \
    ; do curl -fSL "$url" -o "${LIB}/$(basename "$url")"; done
# The job jar (Glue branch selected at runtime via CATALOG_TYPE=glue).
COPY flink/datastream/target/flink-iceberg-bench.jar /opt/flink/usrlib/app.jar
USER flink
