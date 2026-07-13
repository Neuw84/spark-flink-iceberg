# Spark image with Iceberg + S3A + Kafka connector jars.
#
# The jars are pre-downloaded to spark/jars/ on the host (run
# spark/fetch-jars.sh) and COPYed in, rather than curl'd during build: the
# BuildKit sandbox blocks outbound network on this setup, and vendoring the jars
# also makes the image build reproducible offline.
ARG SPARK_VERSION=4.1.2
FROM apache/spark:${SPARK_VERSION}

USER root
COPY jars/ /opt/spark/jars/
WORKDIR /opt/spark/work-dir
