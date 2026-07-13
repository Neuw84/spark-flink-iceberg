# EKS Spark image = spark 4.1 + Iceberg/Glue/Kafka jars + the benchmark jar.
# Includes hadoop-aws + AWS SDK bundle so the streaming checkpoint can live on S3
# (CHECKPOINT_BASE=s3a://…), unlike the local image which checkpoints to disk.
ARG SPARK_VERSION=4.1.2
FROM apache/spark:${SPARK_VERSION}
USER root
# Iceberg/Kafka jars vendored by spark/fetch-jars.sh into spark/jars/.
COPY spark/jars/ /opt/spark/jars/
# hadoop-aws + AWS SDK v2 bundle for s3a:// checkpoints on EKS.
RUN set -eux; for url in \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.2/hadoop-aws-3.4.2.jar" \
      "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.34.0/bundle-2.34.0.jar" \
    ; do curl -fSL "$url" -o "/opt/spark/jars/$(basename "$url")"; done
# Put the app jar on the default Spark jar classpath. Referencing it as
# local:///opt/spark/jars/app.jar avoids the operator 2.5.1 + Spark 4.x
# self-copy bug that hits jars under /opt/spark/work-dir.
COPY spark/scala/target/scala-2.13/spark-iceberg-bench_2.13-1.0.0.jar /opt/spark/jars/app.jar
WORKDIR /opt/spark/work-dir
