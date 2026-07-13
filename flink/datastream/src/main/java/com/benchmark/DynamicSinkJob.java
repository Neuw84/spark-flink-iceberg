package com.benchmark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.sink.dynamic.DynamicIcebergSink;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecord;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecordGenerator;

import java.util.HashMap;
import java.util.Map;

/**
 * Demonstrates Flink's {@link DynamicIcebergSink}: a single stream is routed to
 * a different Iceberg table per {@code event_type} — tables are created and
 * evolved on the fly, no pre-declared table loaders. This is the "dynamic sink"
 * dimension of the feature comparison; Spark has no direct equivalent (you must
 * demultiplex into N writeStreams or use foreachBatch routing).
 *
 * <p>Route: events.event_type in {purchase, cart, view} → demo.streaming.events_&lt;type&gt;
 */
public final class DynamicSinkJob {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        ParamUtil p = ParamUtil.fromArgs(args);
        String bootstrap = p.get("bootstrap", System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "kafka:9092"));
        String topic = p.get("topic", System.getenv().getOrDefault("SOURCE_TOPIC", "events"));
        String catalogUri = p.get("catalog-uri", System.getenv().getOrDefault("CATALOG_URI", "http://iceberg-rest:8181"));
        String warehouse = p.get("warehouse", "s3://" + System.getenv().getOrDefault("WAREHOUSE_BUCKET", "warehouse") + "/");
        String database = p.get("database", System.getenv().getOrDefault("ICEBERG_DB", "streaming"));
        int parallelism = Integer.parseInt(p.get("parallelism", "4"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.enableCheckpointing(Long.parseLong(System.getenv().getOrDefault("CHECKPOINT_INTERVAL_MS", "30000")));

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topic)
                .setGroupId("flink-dynamic-sink-bench")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> raw = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source");

        CatalogLoader catalogLoader = restCatalog(catalogUri, warehouse);

        DynamicIcebergSink.forInput(raw)
                .generator(new EventRouter(database))
                .catalogLoader(catalogLoader)
                .immediateTableUpdate(true) // pick up new columns/tables without a restart
                .append();

        env.execute("flink-dynamic-iceberg-sink");
    }

    /** Routes each JSON event to demo.&lt;db&gt;.events_&lt;event_type&gt;, inferring schema from the record. */
    static final class EventRouter implements DynamicRecordGenerator<String> {
        private final String database;

        EventRouter(String database) {
            this.database = database;
        }

        @Override
        public void generate(String json, org.apache.flink.util.Collector<DynamicRecord> out) throws Exception {
            JsonNode n = MAPPER.readTree(json);
            String type = n.path("event_type").asText("unknown");
            // DynamicRecord carries table identity + inferred schema + row; the
            // sink lazily creates/evolves demo.<db>.events_<type> as needed.
            out.collect(DynamicRecordFactory.fromJson(database, "events_" + type, n));
        }
    }

    private static CatalogLoader restCatalog(String uri, String warehouse) {
        Map<String, String> props = new HashMap<>();
        props.put("uri", uri);
        props.put("warehouse", warehouse);
        props.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        props.put("s3.endpoint", System.getenv().getOrDefault("S3_ENDPOINT", "http://minio:9000"));
        props.put("s3.path-style-access", "true");
        return CatalogLoader.rest("demo", new org.apache.hadoop.conf.Configuration(), props);
    }
}
