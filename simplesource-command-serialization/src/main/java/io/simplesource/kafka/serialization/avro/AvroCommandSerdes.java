package io.simplesource.kafka.serialization.avro;

import io.simplesource.api.CommandId;
import io.simplesource.kafka.api.CommandSerdes;
import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.kafka.model.CommandResponse;
import io.simplesource.kafka.serialization.util.GenericMapper;
import io.simplesource.kafka.serialization.util.GenericSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;

import java.util.Arrays;

import static io.simplesource.kafka.serialization.avro.AvroSpecificGenericMapper.specificDomainMapper;

public final class AvroCommandSerdes<K, C> implements CommandSerdes<K, C> {

    private final Serde<K> ak;
    private final Serde<CommandRequest<K, C>> crq;
    private final Serde<CommandId> crk;
    private final Serde<CommandResponse<K>> crp;

    public static <K extends GenericRecord, C extends GenericRecord> AvroCommandSerdes<K, C> of(
            final String schemaRegistryUrl) {
        return of(schemaRegistryUrl, false);
    }

    public static <K extends GenericRecord, C extends GenericRecord> AvroCommandSerdes<K, C> of(
            final String schemaRegistryUrl,
            final boolean useMockSchemaRegistry
    ) {
        return new AvroCommandSerdes<>(
                specificDomainMapper(),
                specificDomainMapper(),
                schemaRegistryUrl,
                useMockSchemaRegistry);
    }

    public static <K, C> AvroCommandSerdes<K, C> of(
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

    public AvroCommandSerdes(
            final GenericMapper<K, GenericRecord> keyMapper,
            final GenericMapper<C, GenericRecord> commandMapper,
            final String schemaRegistryUrl,
            final boolean useMockSchemaRegistry) {

        Serde<GenericRecord> keySerde = AvroGenericUtils.genericAvroSerde(schemaRegistryUrl, useMockSchemaRegistry, true);
        Serde<GenericRecord> valueSerde = AvroGenericUtils.genericAvroSerde(schemaRegistryUrl, useMockSchemaRegistry, false);

        ak = GenericSerde.of(keySerde, keyMapper::toGeneric, keyMapper::fromGeneric);
        crq = GenericSerde.of(valueSerde,
                v -> AvroSerdes.CommandRequestAvroHelper.toGenericRecord(v.map2(keyMapper::toGeneric, commandMapper::toGeneric)),
                s -> AvroSerdes.CommandRequestAvroHelper.fromGenericRecord(s).map2(keyMapper::fromGeneric, x -> commandMapper.fromGeneric(x)));
        crk = GenericSerde.of(valueSerde,
                AvroSerdes.CommandResponseKeyAvroHelper::toGenericRecord,
                AvroSerdes.CommandResponseKeyAvroHelper::fromGenericRecord);

        crp = GenericSerde.of(valueSerde,
                v -> AvroSerdes.CommandResponseAvroHelper.toCommandResponse(v.map(keyMapper::toGeneric)),
                s -> AvroSerdes.CommandResponseAvroHelper.fromCommandResponse(s).map(keyMapper::fromGeneric));
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
    public Serde<CommandResponse<K>> commandResponse() {
        return crp;
    }

    /**
     * Return the given schema wrapped in a nullable union.
     */
    private static Schema toNullableSchema(final Schema schema) {
        return Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), schema));
    }


}
