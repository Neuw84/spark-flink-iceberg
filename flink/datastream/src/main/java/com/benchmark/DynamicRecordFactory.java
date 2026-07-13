package com.benchmark;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecord;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Builds a {@link DynamicRecord} from an arbitrary JSON node by inferring a flat
 * Iceberg schema (all top-level fields, string/long/double/bool). This is what
 * lets the {@link DynamicSinkJob} accept schema-evolving input: when a v2 event
 * shows up with new columns, the inferred schema grows and the sink evolves the
 * target table (immediateTableUpdate=true).
 *
 * NOTE: the DynamicRecord constructor signature tracks the Iceberg version in
 * .env (1.11.0). If you bump Iceberg and the build breaks here, this factory is
 * the single place to adjust.
 */
final class DynamicRecordFactory {

    private DynamicRecordFactory() {}

    static DynamicRecord fromJson(String database, String tableName, JsonNode n) {
        List<Types.NestedField> fields = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        int fieldId = 1;

        Iterator<String> names = n.fieldNames();
        while (names.hasNext()) {
            String field = names.next();
            JsonNode v = n.get(field);
            if (v.isIntegralNumber()) {
                fields.add(Types.NestedField.optional(fieldId, field, Types.LongType.get()));
                values.add(v.asLong());
            } else if (v.isFloatingPointNumber()) {
                fields.add(Types.NestedField.optional(fieldId, field, Types.DoubleType.get()));
                values.add(v.asDouble());
            } else if (v.isBoolean()) {
                fields.add(Types.NestedField.optional(fieldId, field, Types.BooleanType.get()));
                values.add(v.asBoolean());
            } else {
                fields.add(Types.NestedField.optional(fieldId, field, Types.StringType.get()));
                values.add(StringData.fromString(v.asText("")));
            }
            fieldId++;
        }

        Schema schema = new Schema(fields);
        GenericRowData row = new GenericRowData(values.size());
        for (int i = 0; i < values.size(); i++) {
            row.setField(i, values.get(i));
        }

        TableIdentifier id = TableIdentifier.of(Namespace.of(database), tableName);
        return new DynamicRecord(
                id,
                "main",                    // branch
                schema,
                row,
                PartitionSpec.unpartitioned(),
                DistributionMode.HASH,
                1);                        // write parallelism hint
    }
}
