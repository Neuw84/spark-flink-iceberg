-- ─────────────────────────────────────────────────────────────
-- Flink SQL StatementSet: fan one Kafka stream into three Iceberg tables
-- (one per event_type) in a SINGLE job — the SQL analogue of the DataStream
-- DynamicIcebergSink. All INSERTs share one source scan and one checkpoint,
-- so the tables commit atomically together.
-- ─────────────────────────────────────────────────────────────

SET 'execution.checkpointing.interval' = '30s';

CREATE CATALOG demo WITH (
  'type'                 = 'iceberg',
  'catalog-type'         = 'rest',
  'uri'                  = 'http://iceberg-rest:8181',
  'warehouse'            = 's3://warehouse/',
  'io-impl'              = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint'          = 'http://minio:9000',
  's3.path-style-access' = 'true'
);
CREATE DATABASE IF NOT EXISTS demo.streaming;

CREATE TABLE IF NOT EXISTS demo.streaming.events_purchase (LIKE demo.streaming.events_sql);
CREATE TABLE IF NOT EXISTS demo.streaming.events_cart     (LIKE demo.streaming.events_sql);
CREATE TABLE IF NOT EXISTS demo.streaming.events_view     (LIKE demo.streaming.events_sql);

CREATE TEMPORARY TABLE kafka_events (
  event_id STRING, user_id BIGINT, event_type STRING, product_id BIGINT,
  country STRING, amount DOUBLE, currency STRING, quantity INT,
  event_ts BIGINT, ingest_ts BIGINT, payload STRING
) WITH (
  'connector' = 'kafka', 'topic' = 'events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-sql-routing-bench',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json', 'json.ignore-parse-errors' = 'true'
);

-- StatementSet: all three INSERTs run as one optimized job.
EXECUTE STATEMENT SET
BEGIN
  INSERT INTO demo.streaming.events_purchase SELECT * FROM kafka_events WHERE event_type = 'purchase';
  INSERT INTO demo.streaming.events_cart     SELECT * FROM kafka_events WHERE event_type = 'cart';
  INSERT INTO demo.streaming.events_view     SELECT * FROM kafka_events WHERE event_type = 'view';
END;
