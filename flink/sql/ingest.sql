-- Flink SQL ingest: Kafka JSON -> Iceberg (REST catalog on MinIO).
-- Run with: bench/run_flink_sql.sh (pipes this to sql-client -f).
-- Write distribution mode is a table property (none|hash|range) set below.
-- NOTE: sql-client -f is sensitive to multi-line WITH blocks; each statement is
-- kept on a single line on purpose. Do not reflow.
SET 'execution.checkpointing.interval' = '30s';
SET 'pipeline.name' = 'flink-sql-iceberg-ingest';
CREATE CATALOG demo WITH ('type'='iceberg','catalog-type'='rest','uri'='http://iceberg-rest:8181','warehouse'='s3://warehouse/','io-impl'='org.apache.iceberg.aws.s3.S3FileIO','s3.endpoint'='http://minio:9000','s3.path-style-access'='true');
CREATE DATABASE IF NOT EXISTS demo.streaming;
-- Flink SQL DDL doesn't accept Iceberg transform partitioning (bucket()) — that
-- is a Spark SQL extension. Partition by a plain column (event_type) here; the
-- DataStream job uses bucket(16,user_id). Distribution mode still applies.
CREATE TABLE IF NOT EXISTS demo.streaming.events_sql (event_id STRING, user_id BIGINT, event_type STRING, product_id BIGINT, country STRING, amount DOUBLE, currency STRING, quantity INT, event_ts BIGINT, ingest_ts BIGINT, payload STRING) PARTITIONED BY (event_type) WITH ('format-version'='2','write.distribution-mode'='hash','write.upsert.enabled'='false');
CREATE TEMPORARY TABLE kafka_events (event_id STRING, user_id BIGINT, event_type STRING, product_id BIGINT, country STRING, amount DOUBLE, currency STRING, quantity INT, event_ts BIGINT, ingest_ts BIGINT, payload STRING) WITH ('connector'='kafka','topic'='events','properties.bootstrap.servers'='kafka:9092','properties.group.id'='flink-sql-iceberg-bench','scan.startup.mode'='latest-offset','format'='json','json.ignore-parse-errors'='true');
INSERT INTO demo.streaming.events_sql SELECT event_id, user_id, event_type, product_id, country, amount, currency, quantity, event_ts, ingest_ts, payload FROM kafka_events;
