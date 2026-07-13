#!/usr/bin/env python3
"""PyFlink Table API → Iceberg ingest.

Python twin of the Flink SQL job, expressed through the Table API so it can be
submitted with `flink run -py`. Same Iceberg REST catalog, same distribution
mode as a table property, same checkpoint-bounded latency.
"""
import os

from pyflink.table import EnvironmentSettings, TableEnvironment


def main():
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)
    cfg = t_env.get_config().get_configuration()
    cfg.set_string("execution.checkpointing.interval",
                   f"{int(os.getenv('CHECKPOINT_INTERVAL_MS', '30000'))} ms")

    mode = os.getenv("WRITE_DISTRIBUTION_MODE", "hash")

    t_env.execute_sql(f"""
        CREATE CATALOG demo WITH (
          'type'='iceberg',
          'catalog-type'='rest',
          'uri'='{os.getenv("CATALOG_URI", "http://iceberg-rest:8181")}',
          'warehouse'='s3://{os.getenv("WAREHOUSE_BUCKET", "warehouse")}/',
          'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',
          's3.endpoint'='{os.getenv("S3_ENDPOINT", "http://minio:9000")}',
          's3.path-style-access'='true')
    """)
    t_env.execute_sql("CREATE DATABASE IF NOT EXISTS demo.streaming")
    t_env.execute_sql(f"""
        CREATE TABLE IF NOT EXISTS demo.streaming.events_pyflink (
          event_id STRING, user_id BIGINT, event_type STRING, product_id BIGINT,
          country STRING, amount DOUBLE, currency STRING, quantity INT,
          event_ts BIGINT, ingest_ts BIGINT, payload STRING)
        PARTITIONED BY (event_type)
        WITH ('format-version'='{os.getenv("TABLE_FORMAT_VERSION", "2")}',
              'write.distribution-mode'='{mode}')
    """)

    t_env.execute_sql(f"""
        CREATE TEMPORARY TABLE kafka_events (
          event_id STRING, user_id BIGINT, event_type STRING, product_id BIGINT,
          country STRING, amount DOUBLE, currency STRING, quantity INT,
          event_ts BIGINT, ingest_ts BIGINT, payload STRING)
        WITH ('connector'='kafka', 'topic'='{os.getenv("SOURCE_TOPIC", "events")}',
              'properties.bootstrap.servers'='{os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")}',
              'properties.group.id'='pyflink-iceberg-bench',
              'scan.startup.mode'='latest-offset',
              'format'='json', 'json.ignore-parse-errors'='true')
    """)

    t_env.execute_sql("""
        INSERT INTO demo.streaming.events_pyflink
        SELECT event_id, user_id, event_type, product_id, country,
               amount, currency, quantity, event_ts, ingest_ts, payload
        FROM kafka_events
    """)


if __name__ == "__main__":
    main()
