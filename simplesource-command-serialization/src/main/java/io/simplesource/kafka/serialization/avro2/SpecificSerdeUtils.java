package io.simplesource.kafka.serialization.avro2;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;

import java.util.HashMap;
import java.util.Map;

final public class SpecificSerdeUtils {

    public static <T extends SpecificRecord> Serde<T> specificAvroSerde(String registryUrl, Boolean isSerdeForRecordKeys) {
        return specificAvroSerde(registryUrl, isSerdeForRecordKeys, null);
    }

    public static <T extends SpecificRecord> Serde<T> specificAvroSerde(String registryUrl, Boolean isSerdeForRecordKeys, SchemaRegistryClient registryClient) {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
        final Serde<T> serde = (Serde<T>)(registryClient != null ? new SpecificAvroSerde<>(registryClient) : new SpecificAvroSerde<>());
        serde.configure(configMap, isSerdeForRecordKeys);
        return serde;
    }
}
