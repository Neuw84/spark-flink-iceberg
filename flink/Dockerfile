# Flink image with the Iceberg + S3 + Kafka jars baked in.
ARG FLINK_VERSION=2.2.0
FROM flink:${FLINK_VERSION}-java17

ARG ICEBERG_VERSION=1.11.0
ARG FLINK_MINOR=2.0

# Python + PyFlink so `flink run -py` works (the flink image ships no python).
# The base image runs as user `flink`; switch to root for apt/pip then back.
USER root
# pemja (a PyFlink build dep) compiles against JNI headers, which the Flink
# image's JRE lacks. Install a full JDK and point JAVA_HOME at it for the build.
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends python3 python3-pip python3-dev build-essential openjdk-17-jdk-headless; \
    ln -sf /usr/bin/python3 /usr/bin/python; \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture) \
      pip3 install --no-cache-dir --break-system-packages --prefer-binary apache-flink==2.2.0; \
    apt-get purge -y build-essential python3-dev openjdk-17-jdk-headless; apt-get autoremove -y; \
    rm -rf /var/lib/apt/lists/*

# S3 filesystem plugin (talks to MinIO locally, S3 on EKS).
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/

# Iceberg + Kafka connector + AWS bundle into the Flink classpath.
ENV LIB=/opt/flink/lib
RUN set -eux; \
    for url in \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-${FLINK_MINOR}/${ICEBERG_VERSION}/iceberg-flink-runtime-${FLINK_MINOR}-${ICEBERG_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/5.0.0-2.2/flink-connector-kafka-5.0.0-2.2.jar" \
      "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.0/kafka-clients-3.9.0.jar" \
      "https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar" \
    ; do curl -fSL "$url" -o "${LIB}/$(basename "$url")"; done
# Hadoop (org.apache.hadoop.conf.Configuration, needed by Iceberg's CatalogLoader)
# is provided on the cluster classpath via the flink-shaded-hadoop uber jar above.
# Flink 2.x images no longer ship Hadoop. Kept as a provided dep in the job pom.

# Restore the default non-root user the base image runs as.
USER flink

# The benchmark fat-jar is injected at runtime by bench scripts via `docker cp`
# (see bench/run_flink_datastream.sh), so it is not COPYed at build time.
