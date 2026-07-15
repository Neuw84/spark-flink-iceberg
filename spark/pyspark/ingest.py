#!/usr/bin/env python3
"""PySpark Structured Streaming -> Iceberg ingest benchmark.

Python twin of the Scala IcebergIngestJob, at full feature parity so the EKS
matrix can run on PySpark instead of Scala. Same table, same trigger semantics,
same Kafka fetch tuning, same write-distribution/partitioning knobs — so the only
variable vs the Scala job is the driver language (the executor data path is pure
JVM Catalyst either way: from_json is a Catalyst expression, no Python in the
per-record path).

Kafka (JSON) -> parse -> Iceberg table via the DataSourceV2 streaming writer
(append) or foreachBatch + MERGE INTO (upsert).

Args (all optional, env fallbacks): --distribution-mode none|hash|range
  --upsert true|false  --partitioning bucket|time  --format-version 2|3
  --merge-mode merge-on-read|copy-on-write  --trigger <seconds>  --buckets N
"""
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, row_number
from pyspark.sql.window import Window
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
    """Parse a --flag value pair from argv, falling back to default."""
    return dict(zip(sys.argv[1::2], sys.argv[2::2])).get("--" + flag, default)


def env(key, default):
    return os.getenv(key, default)


def main():
    bootstrap = env("KAFKA_BOOTSTRAP", "kafka:9092")
    topic = env("SOURCE_TOPIC", "events")
    db = env("ICEBERG_DB", "streaming")
    # Table name is env-driven so the EKS run script and this job agree on the
    # physical table. Suffix defaults to _pyspark locally (coexists with the Scala
    # job's _spark table); the EKS manifest sets ICEBERG_TABLE_SUFFIX=_spark so the
    # measurement in 04_run_benchmark.sh (which reads events_spark) matches.
    table = env("ICEBERG_TABLE", "events") + env("ICEBERG_TABLE_SUFFIX", "_pyspark")
    mode = arg("distribution-mode", env("WRITE_DISTRIBUTION_MODE", "hash"))
    upsert = arg("upsert", "false").lower() == "true"
    trigger_sec = int(arg("trigger",
                          str(max(1, int(env("CHECKPOINT_INTERVAL_MS", "30000")) // 1000))))
    warehouse = env("WAREHOUSE_BUCKET", "warehouse")
    checkpoint_base = env("CHECKPOINT_BASE", "file:///tmp/spark-ckpt-py")
    # partitioning: "bucket" = bucket(N,user_id) | "time" = identity on epoch-minute
    partitioning = arg("partitioning", env("PARTITIONING", "bucket"))
    # bucket count = concurrent write streams (throughput lever); default 48 to match
    # the Flink job so the write-parallelism knob is held equal.
    buckets = arg("buckets", env("BUCKETS", "48"))
    format_version = arg("format-version", env("TABLE_FORMAT_VERSION", "2"))
    # Upsert engine mode: merge-on-read (deletion vectors) vs copy-on-write. Ignored
    # for append. Mirrors the Scala job so Spark's MoR/CoW both compare to Flink DV.
    merge_mode = arg("merge-mode", env("WRITE_MERGE_MODE", "merge-on-read"))
    partition_clause = "event_minute" if partitioning == "time" else f"bucket({buckets}, user_id)"

    # Catalog config source, same rule as the Scala job:
    #  * EKS (Spark operator): CONFIG_FROM_MANIFEST=1 -> all spark.sql.catalog.demo.*
    #    come from the manifest sparkConf (Glue + S3, us-east-1). We must NOT re-set
    #    them here, so we don't fight the operator-provided SparkConf.
    #  * Local: set them in code (Iceberg REST catalog on MinIO).
    config_from_manifest = env("CONFIG_FROM_MANIFEST", "0") == "1"
    catalog_type = env("CATALOG_TYPE", "rest")

    builder = (SparkSession.builder
               .appName(f"pyspark-structured-streaming-iceberg[{mode}{',upsert' if upsert else ''}]"))
    if not config_from_manifest:
        builder = (builder
                   .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                   .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
                   .config("spark.sql.catalog.demo.type", catalog_type)
                   .config("spark.sql.catalog.demo.warehouse", f"s3://{warehouse}/")
                   .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"))
        if catalog_type == "rest":
            builder = (builder
                       .config("spark.sql.catalog.demo.uri", env("CATALOG_URI", "http://iceberg-rest:8181"))
                       .config("spark.sql.catalog.demo.s3.endpoint", env("S3_ENDPOINT", "http://minio:9000"))
                       .config("spark.sql.catalog.demo.s3.path-style-access", "true"))
    spark = builder.getOrCreate()

    fq = f"demo.{db}.{table}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS demo.{db}")
    # Clean per-run state (RECREATE_TABLE=1): plain metadata DROP — per-run
    # measurement reads the current snapshot, so orphaned data files from a prior run
    # aren't counted. (Matches the Scala job; PURGE hits a DSv2 code path that
    # NoSuchMethodErrors on iceberg-1.11 / Spark 4.1.2.)
    if env("RECREATE_TABLE", "0") == "1":
        spark.sql(f"DROP TABLE IF EXISTS {fq}")
    # For upsert, pin delete/update/merge modes to the chosen engine (MoR vs CoW).
    merge_props = ("" if not upsert else
                   f", 'write.delete.mode'='{merge_mode}'"
                   f", 'write.update.mode'='{merge_mode}'"
                   f", 'write.merge.mode'='{merge_mode}'")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {fq} (
        event_id string, user_id bigint, event_type string, product_id bigint,
        country string, amount double, currency string, quantity int,
        event_ts bigint, ingest_ts bigint, payload string,
        event_time timestamp, event_minute bigint)
      USING iceberg
      PARTITIONED BY ({partition_clause})
      TBLPROPERTIES (
        'format-version'='{format_version}',
        'write.distribution-mode'='{mode}'{merge_props})""")

    # maxOffsetsPerTrigger UNSET by default so Spark drains as fast as it can each
    # micro-batch — matching Flink's uncapped continuous read for a fair overload
    # test. Set MAX_OFFSETS_PER_TRIGGER only to deliberately throttle. (The old
    # PySpark twin hard-capped this at 3M, which is NOT parity with Flink.)
    max_offsets = env("MAX_OFFSETS_PER_TRIGGER", "")

    reader = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", bootstrap)
              .option("subscribe", topic)
              .option("startingOffsets", arg("starting-offsets", env("STARTING_OFFSETS", "latest")))
              # aggressive Kafka retention can purge offsets a stale checkpoint
              # expects; don't fail the query on that.
              .option("failOnDataLoss", "false")
              # Kafka consumer fetch tuning — kafka.*-prefixed options go straight to
              # the consumer. MIRRORED VERBATIM from the Flink job (.setProperty) so
              # the read path is at parity; both otherwise run on identical stock
              # Kafka defaults (fetch.min.bytes=1, max.partition.fetch.bytes=1MiB,
              # fetch.max.bytes=50MiB, receive.buffer.bytes=64KiB, max.poll.records=500).
              .option("kafka.fetch.min.bytes", env("KAFKA_FETCH_MIN_BYTES", "1048576"))
              .option("kafka.fetch.max.wait.ms", env("KAFKA_FETCH_MAX_WAIT_MS", "500"))
              .option("kafka.max.partition.fetch.bytes", env("KAFKA_MAX_PARTITION_FETCH_BYTES", "8388608"))
              .option("kafka.fetch.max.bytes", env("KAFKA_FETCH_MAX_BYTES", "67108864"))
              .option("kafka.receive.buffer.bytes", env("KAFKA_RECEIVE_BUFFER_BYTES", "2097152"))
              .option("kafka.max.poll.records", env("KAFKA_MAX_POLL_RECORDS", "5000")))
    if max_offsets:
        reader = reader.option("maxOffsetsPerTrigger", max_offsets)
    raw = reader.load()

    parsed = (raw
              .select(from_json(col("value").cast("string"), SCHEMA).alias("e"))
              .select("e.*")
              # derive a real timestamp + an epoch-minute bucket for minute-granular
              # identity time-partitioning (Iceberg has no minute() transform).
              .withColumn("event_time", (col("event_ts") / 1000).cast("timestamp"))
              .withColumn("event_minute", (col("event_ts") / 60000).cast("long")))

    if upsert:
        # Upsert path: MERGE INTO on each micro-batch keyed by user_id (same equality
        # key as Flink's .upsert()). Iceberg MERGE errors on >1 source row per target
        # row, so dedup the batch to the latest event per user_id first.
        def upsert_batch(batch, _batch_id):
            deduped = (batch
                       .withColumn("_rn", row_number().over(
                           Window.partitionBy("user_id").orderBy(col("ingest_ts").desc())))
                       .filter(col("_rn") == 1).drop("_rn"))
            deduped.createOrReplaceTempView("updates")
            batch.sparkSession.sql(
                f"""MERGE INTO {fq} t
                    USING (SELECT * FROM updates) s
                    ON t.user_id = s.user_id
                    WHEN MATCHED THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *""")

        query = (parsed.writeStream
                 .foreachBatch(upsert_batch)
                 .option("checkpointLocation", f"{checkpoint_base}/{table}")
                 .trigger(processingTime=f"{trigger_sec} seconds")
                 .start())
    else:
        # Append path: native DataSourceV2 streaming writer.
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
