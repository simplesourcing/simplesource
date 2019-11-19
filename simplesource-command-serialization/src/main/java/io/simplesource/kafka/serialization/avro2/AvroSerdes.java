package io.simplesource.kafka.serialization.avro2;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.simplesource.kafka.api.AggregateSerdes;
import io.simplesource.kafka.api.CommandSerdes;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;

final public class AvroSerdes {

    public static <K, C> CommandSerdes<K, C> commandSerdes(
            final Serde<K> keySerde,
            final Serde<C> commandSerde,
            final String schemaRegistryUrl,
            final boolean useMockSchemaRegistry) {
        return new AvroCommandSerdes<>(keySerde, commandSerde, schemaRegistryUrl, useMockSchemaRegistry);
    }

    public static <K, C, E, A> AggregateSerdes<K, C, E, A> aggregateSerdes(
            final Serde<K> keySerde,
            final Serde<C> commandSerde,
            final Serde<E> eventSerde,
            final Serde<A> aggregateSerde,
            final String schemaRegistryUrl,
            final boolean useMockSchemaRegistry) {
        return new AvroAggregateSerdes<>(keySerde, commandSerde, eventSerde, aggregateSerde, schemaRegistryUrl, useMockSchemaRegistry);
    }

    public interface Specific {
         static <K extends SpecificRecord, C extends SpecificRecord> CommandSerdes<K, C> commandSerdes(
                 final String schemaRegistryUrl) {
            return commandSerdes(schemaRegistryUrl, false);
        }

        static <K extends SpecificRecord, C extends SpecificRecord, E extends SpecificRecord, A extends SpecificRecord> AggregateSerdes<K, C, E, A> aggregateSerdes(
                final String schemaRegistryUrl) {
            return aggregateSerdes(schemaRegistryUrl, false);
        }

        static <K extends SpecificRecord, C extends SpecificRecord> CommandSerdes<K, C> commandSerdes(
                final String schemaRegistryUrl,
                final boolean useMockSchemaRegistry) {
            SchemaRegistryClient regClient = useMockSchemaRegistry ? new MockSchemaRegistryClient() : null;
            return AvroSerdes.commandSerdes(
                    SpecificSerdeUtils.specificAvroSerde(schemaRegistryUrl, true, regClient),
                    SpecificSerdeUtils.specificAvroSerde(schemaRegistryUrl, false, regClient),
                    schemaRegistryUrl, useMockSchemaRegistry);
        }

        static <K extends SpecificRecord, C extends SpecificRecord, E extends SpecificRecord, A extends SpecificRecord> AggregateSerdes<K, C, E, A> aggregateSerdes(
                final String schemaRegistryUrl,
                final boolean useMockSchemaRegistry) {
            SchemaRegistryClient regClient = useMockSchemaRegistry ? new MockSchemaRegistryClient() : null;
            return AvroSerdes.aggregateSerdes(
                    SpecificSerdeUtils.specificAvroSerde(schemaRegistryUrl, true, regClient),
                    SpecificSerdeUtils.specificAvroSerde(schemaRegistryUrl, false, regClient),
                    SpecificSerdeUtils.specificAvroSerde(schemaRegistryUrl, false, regClient),
                    SpecificSerdeUtils.specificAvroSerde(schemaRegistryUrl, false, regClient),
                    schemaRegistryUrl, useMockSchemaRegistry);
        }
    }

    public interface Generic {
        static <K extends GenericRecord, C extends GenericRecord> CommandSerdes<K, C> commandSerdes(
                final String schemaRegistryUrl) {
            return commandSerdes(schemaRegistryUrl, false);
        }

        static <K extends GenericRecord, C extends GenericRecord, E extends GenericRecord, A extends GenericRecord> AggregateSerdes<K, C, E, A> aggregateSerdes(
                final String schemaRegistryUrl) {
            return aggregateSerdes(schemaRegistryUrl, false);
        }

        static <K extends GenericRecord, C extends GenericRecord> CommandSerdes<K, C> commandSerdes(
                final String schemaRegistryUrl,
                final boolean useMockSchemaRegistry) {
            SchemaRegistryClient regClient = useMockSchemaRegistry ? new MockSchemaRegistryClient() : null;
            return AvroSerdes.commandSerdes(
                    GenericSerdeUtils.genericAvroSerde(schemaRegistryUrl, true, regClient),
                    GenericSerdeUtils.genericAvroSerde(schemaRegistryUrl, false, regClient),
                    schemaRegistryUrl,
                    useMockSchemaRegistry);
        }

        static <K extends GenericRecord, C extends GenericRecord, E extends GenericRecord, A extends GenericRecord> AggregateSerdes<K, C, E, A> aggregateSerdes(
                final String schemaRegistryUrl,
                final boolean useMockSchemaRegistry) {
            SchemaRegistryClient regClient = useMockSchemaRegistry ? new MockSchemaRegistryClient() : null;
            return AvroSerdes.aggregateSerdes(
                    GenericSerdeUtils.genericAvroSerde(schemaRegistryUrl, true, regClient),
                    GenericSerdeUtils.genericAvroSerde(schemaRegistryUrl, false, regClient),
                    GenericSerdeUtils.genericAvroSerde(schemaRegistryUrl, false, regClient),
                    GenericSerdeUtils.genericAvroSerde(schemaRegistryUrl, false, regClient),
                    schemaRegistryUrl, useMockSchemaRegistry);
        }
    }
}
