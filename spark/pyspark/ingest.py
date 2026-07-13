#!/usr/bin/env python3
"""PySpark Structured Streaming → Iceberg ingest.

Python twin of the Scala IcebergIngestJob. Same table, same trigger semantics,
so the Python-vs-JVM overhead is directly comparable at identical config.

Submit via bench/run_pyspark.sh (which sets --packages / catalog configs).
"""
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType)

SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", LongType()),
    StructField("event_type", StringType()),
    StructField("product_id", LongType()),
    StructField("country", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("quantity", IntegerType()),
    StructField("event_ts", LongType()),
    StructField("ingest_ts", LongType()),
    StructField("payload", StringType()),
])


def arg(flag, default):
    return dict(zip(sys.argv[1::2], sys.argv[2::2])).get("--" + flag, default)


def main():
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    topic = os.getenv("SOURCE_TOPIC", "events")
    db = os.getenv("ICEBERG_DB", "streaming")
    table = os.getenv("ICEBERG_TABLE", "events") + "_pyspark"
    mode = arg("distribution-mode", os.getenv("WRITE_DISTRIBUTION_MODE", "hash"))
    trigger_sec = int(arg("trigger", str(max(1, int(os.getenv("CHECKPOINT_INTERVAL_MS", "30000")) // 1000))))
    warehouse = os.getenv("WAREHOUSE_BUCKET", "warehouse")
    # Iceberg data IO uses S3FileIO; Spark's streaming checkpoint uses Hadoop FS.
    # Keep it local by default (EKS sets CHECKPOINT_BASE=s3a://… with hadoop-aws).
    checkpoint_base = os.getenv("CHECKPOINT_BASE", "file:///tmp/spark-ckpt-py")

    spark = (SparkSession.builder
             .appName(f"pyspark-structured-streaming-iceberg[{mode}]")
             .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
             .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
             .config("spark.sql.catalog.demo.type", "rest")
             .config("spark.sql.catalog.demo.uri", os.getenv("CATALOG_URI", "http://iceberg-rest:8181"))
             .config("spark.sql.catalog.demo.warehouse", f"s3://{warehouse}/")
             .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
             .config("spark.sql.catalog.demo.s3.endpoint", os.getenv("S3_ENDPOINT", "http://minio:9000"))
             .config("spark.sql.catalog.demo.s3.path-style-access", "true")
             .getOrCreate())

    fq = f"demo.{db}.{table}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS demo.{db}")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {fq} (
        event_id string, user_id bigint, event_type string, product_id bigint,
        country string, amount double, currency string, quantity int,
        event_ts bigint, ingest_ts bigint, payload string)
      USING iceberg
      PARTITIONED BY (bucket(16, user_id))
      TBLPROPERTIES (
        'format-version'='{os.getenv("TABLE_FORMAT_VERSION", "2")}',
        'write.distribution-mode'='{mode}')""")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", bootstrap)
           .option("subscribe", topic)
           .option("startingOffsets", "latest")
           .option("maxOffsetsPerTrigger", os.getenv("MAX_OFFSETS_PER_TRIGGER", "3000000"))
           .load())

    parsed = raw.select(from_json(col("value").cast("string"), SCHEMA).alias("e")).select("e.*")

    query = (parsed.writeStream
             .format("iceberg")
             .outputMode("append")
             .option("checkpointLocation", f"{checkpoint_base}/{table}")
             .option("fanout-enabled", "true")
             .trigger(processingTime=f"{trigger_sec} seconds")
             .toTable(fq))
    query.awaitTermination()


if __name__ == "__main__":
    main()
