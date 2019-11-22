package io.simplesource.kafka.serialization.avro;

import io.simplesource.kafka.api.EventSerdes;
import io.simplesource.kafka.serialization.util.GenericMapper;
import io.simplesource.serialization.avro.generated.AvroBool;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public class AvroEventSerdes<K, E> extends AvroAggregateSerdes<K, E, E, Boolean> implements EventSerdes<K, E> {
    AvroEventSerdes(
            final GenericMapper<K, GenericRecord> keyMapper,
            final GenericMapper<E, GenericRecord> eventMapper,
            final String schemaRegistryUrl,
            final boolean useMockSchemaRegistry,
            final Schema aggregateSchema) {
        super(keyMapper, eventMapper, eventMapper,
                GenericMapper.of(AvroBool::new, s -> (Boolean) s.get("value")), schemaRegistryUrl,
         useMockSchemaRegistry,
         aggregateSchema);
    }
}
