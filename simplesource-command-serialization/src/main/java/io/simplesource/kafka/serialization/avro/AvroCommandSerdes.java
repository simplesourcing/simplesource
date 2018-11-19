package io.simplesource.kafka.serialization.avro;

import io.simplesource.kafka.api.CommandSerdes;
import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.kafka.model.CommandResponse;
import io.simplesource.kafka.serialization.util.GenericMapper;
import io.simplesource.kafka.serialization.util.GenericSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;

import java.util.Arrays;
import java.util.UUID;

import static io.simplesource.kafka.serialization.avro.AvroSpecificGenericMapper.specificDomainMapper;

public final class AvroCommandSerdes<K, C> implements CommandSerdes<K, C> {

    private final Serde<K> ak;
    private final Serde<CommandRequest<K, C>> crq;
    private final Serde<UUID> crk;
    private final Serde<CommandResponse> crp;

    public static <A extends GenericRecord, E extends GenericRecord, C extends GenericRecord, K extends GenericRecord> AvroCommandSerdes<K, C> of(
            final String schemaRegistryUrl) {
        return of(schemaRegistryUrl, false);
    }

    public static <A extends GenericRecord, E extends GenericRecord, C extends GenericRecord, K extends GenericRecord> AvroCommandSerdes<K, C> of(
            final String schemaRegistryUrl,
            final boolean useMockSchemaRegistry
    ) {
        return new AvroCommandSerdes<>(
                specificDomainMapper(),
                specificDomainMapper(),
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
                AvroSerdes.CommandResponseAvroHelper::toCommandResponse,
                AvroSerdes.CommandResponseAvroHelper::fromCommandResponse);
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
    public Serde<UUID> commandResponseKey() {
        return crk;
    }

    @Override
    public Serde<CommandResponse> commandResponse() {
        return crp;
    }

    /**
     * Return the given schema wrapped in a nullable union.
     */
    private static Schema toNullableSchema(final Schema schema) {
        return Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), schema));
    }


}
