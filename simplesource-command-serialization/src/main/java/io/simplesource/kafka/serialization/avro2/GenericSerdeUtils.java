
package io.simplesource.kafka.serialization.avro2;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;

import java.util.HashMap;
import java.util.Map;

final public class GenericSerdeUtils {

    public static <T extends GenericRecord> Serde<T> genericAvroSerde(String schemaRegistryUrl, Boolean isSerdeForRecordKeys) {
        return genericAvroSerde(schemaRegistryUrl, isSerdeForRecordKeys, null);
    }

    public static <T extends GenericRecord> Serde<T> genericAvroSerde(String schemaRegistryUrl, Boolean isSerdeForRecordKeys, SchemaRegistryClient registryClient) {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        final Serde<T> serde = (Serde<T>)(registryClient != null ? new GenericAvroSerde(registryClient) : new GenericAvroSerde());
        serde.configure(configMap, isSerdeForRecordKeys);
        return serde;
    }
}
