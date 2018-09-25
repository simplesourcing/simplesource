package io.simplesource.kafka.serialization.avro;

import io.simplesource.data.Sequence;
import io.simplesource.kafka.model.ValueWithSequence;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.common.serialization.Serde;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class AvroGenericUtils {

    public enum SchemaNameStrategy {
        TOPIC_NAME,
        TOPIC_RECORD_NAME
    }

    public static Serde<GenericRecord> genericAvroSerde(
            final String schemaRegistryUrl,
            final boolean useMockSchemaRegistry,
            final boolean isKey) {
        return genericAvroSerde(schemaRegistryUrl, useMockSchemaRegistry, isKey, SchemaNameStrategy.TOPIC_RECORD_NAME);
    }

    public static Serde<GenericRecord> genericAvroSerde(
            final String schemaRegistryUrl,
            final boolean useMockSchemaRegistry,
            final boolean isKey,
            final SchemaNameStrategy schemaNameStrategy) {
        final Map<String, Object> configMap = avroSchemaRegistryConfig(schemaRegistryUrl, schemaNameStrategy);
        final Serde<GenericRecord> serde = useMockSchemaRegistry
                ? new GenericAvroSerde(new MockSchemaRegistryClient())
                : new GenericAvroSerde();
        serde.configure(configMap, isKey);
        return serde;
    }

    private static Map<String, Object> avroSchemaRegistryConfig(String schemaRegistryUrl, SchemaNameStrategy schemaNameStrategy) {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        if (schemaNameStrategy == SchemaNameStrategy.TOPIC_RECORD_NAME) {
            configMap.put(AbstractKafkaAvroSerDeConfig.KEY_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class);
            configMap.put(AbstractKafkaAvroSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class);
        }
        return configMap;
    }

    public static class ValueWithSequenceAvroHelper {
        private static final Map<Schema, Schema> schemaCache = new ConcurrentHashMap<>();

        private static final String VALUE = "value";
        private static final String SEQUENCE = "sequence";

        public static GenericRecord toGenericRecord(
                final ValueWithSequence<GenericRecord> valueWithSequence
        ) {
            final GenericRecord value = valueWithSequence.value();
            final Schema schema = schemaCache.computeIfAbsent(value.getSchema(),
                    k -> valueWithSequenceSchema(value));
            final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
            return builder
                    .set(VALUE, value)
                    .set(SEQUENCE, valueWithSequence.sequence().getSeq())
                    .build();
        }

        public static ValueWithSequence<GenericRecord> fromGenericRecord(final GenericRecord record) {
            final GenericRecord genericValue = (GenericRecord) record.get(VALUE);
            final Sequence sequence = Sequence.position((Long) record.get(SEQUENCE));

            return new ValueWithSequence<>(genericValue, sequence);
        }

        private static Schema valueWithSequenceSchema(final GenericRecord value) {
            return SchemaBuilder
                    .record(value.getSchema().getName() + "ValueWithSequence").namespace(value.getClass().getPackage().getName())
                    .fields()
                    .name(VALUE).type(value.getSchema()).noDefault()
                    .name(SEQUENCE).type().longType().noDefault()
                    .endRecord();
        }
    }
}
