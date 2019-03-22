package io.simplesource.kafka.serialization.avro;

import io.simplesource.api.CommandId;
import io.simplesource.kafka.api.AggregateSerdes;
import io.simplesource.kafka.serialization.util.GenericMapper;
import io.simplesource.kafka.model.*;
import io.simplesource.kafka.serialization.util.GenericSerde;
import io.simplesource.kafka.serialization.avro.AvroGenericUtils.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;

import java.util.*;

import static io.simplesource.kafka.serialization.avro.AvroSpecificGenericMapper.specificDomainMapper;
import static io.simplesource.kafka.serialization.avro.AvroSerdes.*;

public final class AvroAggregateSerdes<K, C, E, A> implements AggregateSerdes<K, C, E, A> {

    private final Serde<K> ak;
    private final Serde<CommandRequest<K, C>> crq;
    private final Serde<CommandId> crk;
    private final Serde<ValueWithSequence<E>> vws;
    private final Serde<AggregateUpdate<A>> au;
    private final Serde<CommandResponse<K>> crp;

    public static <K extends GenericRecord, C extends GenericRecord, E extends GenericRecord, A extends GenericRecord> AvroAggregateSerdes<K, C, E, A> of(
            final String schemaRegistryUrl,
            final Schema aggregateSchema
    ) {
        return of(
                schemaRegistryUrl,
                false,
                aggregateSchema);
    }

    public static <K extends GenericRecord, C extends GenericRecord, E extends GenericRecord, A extends GenericRecord> AvroAggregateSerdes<K, C, E, A> of(
            final String schemaRegistryUrl,
            final boolean useMockSchemaRegistry,
            final Schema aggregateSchema
    ) {
        return new AvroAggregateSerdes<>(
                specificDomainMapper(),
                specificDomainMapper(),
                specificDomainMapper(),
                specificDomainMapper(),
                schemaRegistryUrl,
                useMockSchemaRegistry,
                aggregateSchema);
    }

    public AvroAggregateSerdes(
            final GenericMapper<K, GenericRecord> keyMapper,
            final GenericMapper<C, GenericRecord> commandMapper,
            final GenericMapper<E, GenericRecord> eventMapper,
            final GenericMapper<A, GenericRecord> aggregateMapper,
            final String schemaRegistryUrl,
            final boolean useMockSchemaRegistry,
            final Schema aggregateSchema) {

        // aggregates must use SchemaNameStrategy.TOPIC_NAME for compatibility with kafka-connect
        // (so aggregate topics can be streamed directly to the read-store)
        Serde<GenericRecord> aggKeySerde = AvroGenericUtils.genericAvroSerde(schemaRegistryUrl, useMockSchemaRegistry, true, SchemaNameStrategy.TOPIC_NAME);
        Serde<GenericRecord> aggValueSerde = AvroGenericUtils.genericAvroSerde(schemaRegistryUrl, useMockSchemaRegistry, false, SchemaNameStrategy.TOPIC_NAME);

        // commands, events, responses etc. can have records of different schema records going through the same topic.
        // SchemaNameStrategy.TOPIC_RECORD_NAME is required to prevent name conflicts in schema registry
        Serde<GenericRecord> valueSerde = AvroGenericUtils.genericAvroSerde(schemaRegistryUrl, useMockSchemaRegistry, false, SchemaNameStrategy.TOPIC_RECORD_NAME);

        ak = GenericSerde.of(aggKeySerde, keyMapper::toGeneric, keyMapper::fromGeneric);
        crq = GenericSerde.of(valueSerde,
                v -> CommandRequestAvroHelper.toGenericRecord(v.map2(keyMapper::toGeneric, commandMapper::toGeneric)),
                s -> CommandRequestAvroHelper.fromGenericRecord(s).map2(keyMapper::fromGeneric, commandMapper::fromGeneric));
        crk = GenericSerde.of(valueSerde,
                CommandResponseKeyAvroHelper::toGenericRecord,
                CommandResponseKeyAvroHelper::fromGenericRecord);
        vws = GenericSerde.of(valueSerde,
                v -> AvroGenericUtils.ValueWithSequenceAvroHelper.toGenericRecord(v.map(eventMapper::toGeneric)),
                s -> AvroGenericUtils.ValueWithSequenceAvroHelper.fromGenericRecord(s).map(eventMapper::fromGeneric));
        au = GenericSerde.of(aggValueSerde,
                v -> AggregateUpdateAvroHelper.toGenericRecord(v.map(aggregateMapper::toGeneric), aggregateSchema),
                s -> AggregateUpdateAvroHelper.fromGenericRecord(s)
                        .map(aggregateMapper::fromGeneric));
        crp = GenericSerde.of(valueSerde,
                v -> CommandResponseAvroHelper.toCommandResponse(v.map(keyMapper::toGeneric)),
                s -> CommandResponseAvroHelper.fromCommandResponse(s).map(keyMapper::fromGeneric));
    }

    @Override
    public Serde<K> aggregateKey() {
        return ak;
    }

    @Override
    public Serde<CommandRequest<K, C>> commandRequest() {
        return crq;
    }

    @Override
    public Serde<CommandId> commandResponseKey() {
        return crk;
    }

    @Override
    public Serde<ValueWithSequence<E>> valueWithSequence() {
        return vws;
    }

    @Override
    public Serde<AggregateUpdate<A>> aggregateUpdate() {
        return au;
    }

    @Override
    public Serde<CommandResponse<K>> commandResponse() {
        return crp;
    }

}
