package io.simplesource.kafka.serialization.avro;

import io.simplesource.kafka.api.AggregateSerdes;
import io.simplesource.kafka.api.CommandSerdes;
import io.simplesource.kafka.serialization.util.GenericMapper;
import io.simplesource.serialization.avro.generated.AvroBool;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import static io.simplesource.kafka.serialization.avro.AvroSpecificGenericMapper.specificDomainMapper;

public final class AvroSerdes {
    public static class Custom {
        public static <K, C> CommandSerdes<K, C> command(
                final GenericMapper<K, GenericRecord> keyMapper,
                final GenericMapper<C, GenericRecord> commandMapper,
                final String schemaRegistryUrl,
                final boolean useMockSchemaRegistry
        ) {
            return new AvroCommandSerdes<>(
                    keyMapper,
                    commandMapper,
                    schemaRegistryUrl,
                    useMockSchemaRegistry);
        }

        public static <K, C, E, A> AggregateSerdes<K, C, E, A> aggregate(
                final GenericMapper<K, GenericRecord> keyMapper,
                final GenericMapper<C, GenericRecord> commandMapper,
                final GenericMapper<E, GenericRecord> eventMapper,
                final GenericMapper<A, GenericRecord> aggregateMapper,
                final String schemaRegistryUrl,
                final boolean useMockSchemaRegistry,
                final Schema aggregateSchema
        ) {
            return new AvroAggregateSerdes<>(
                    keyMapper,
                    commandMapper,
                    eventMapper,
                    aggregateMapper,
                    schemaRegistryUrl,
                    useMockSchemaRegistry,
                    aggregateSchema);
        }

        public static <K, E> AggregateSerdes<K, E, E, Boolean> event(
                final GenericMapper<K, GenericRecord> keyMapper,
                final GenericMapper<E, GenericRecord> eventMapper,
                final String schemaRegistryUrl,
                final boolean useMockSchemaRegistry
        ) {
            return new AvroAggregateSerdes<>(
                    keyMapper,
                    eventMapper,
                    eventMapper,
                    GenericMapper.of(AvroBool::new, s -> (Boolean) s.get("value")),
                    schemaRegistryUrl,
                    useMockSchemaRegistry,
                    AvroBool.SCHEMA$);
        }

    }

    public static class Generic {
        public static <K extends GenericRecord, C extends GenericRecord> CommandSerdes<K, C> command(
                final String schemaRegistryUrl,
                final boolean useMockSchemaRegistry) {
            return new AvroCommandSerdes<>(specificDomainMapper(), specificDomainMapper(), schemaRegistryUrl, useMockSchemaRegistry);
        }

        public static <K extends GenericRecord, C extends GenericRecord, E extends GenericRecord, A extends GenericRecord> AggregateSerdes<K, C, E, A> aggregate(
                final String schemaRegistryUrl,
                final boolean useMockSchemaRegistry,
                final Schema aggregateSchema) {
            return new AvroAggregateSerdes<>(specificDomainMapper(), specificDomainMapper(), specificDomainMapper(), specificDomainMapper(), schemaRegistryUrl, useMockSchemaRegistry, aggregateSchema);
        }

        public static <K extends GenericRecord, E extends GenericRecord> AggregateSerdes<K, E, E, Boolean> event(
                final String schemaRegistryUrl,
                final boolean useMockSchemaRegistry
        ) {
            return new AvroAggregateSerdes<>(
                    specificDomainMapper(),
                    specificDomainMapper(),
                    specificDomainMapper(),
                    GenericMapper.of(AvroBool::new, s -> (Boolean) s.get("value")),
                    schemaRegistryUrl,
                    useMockSchemaRegistry,
                    AvroBool.SCHEMA$);
        }
    }
}
