package io.simplesource.kafka.serialization.avro;

import io.simplesource.api.CommandError;
import io.simplesource.api.CommandError.Reason;
import io.simplesource.data.Sequence;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.kafka.api.AggregateSerdes;
import io.simplesource.kafka.serialization.util.GenericMapper;
import io.simplesource.kafka.model.*;
import io.simplesource.kafka.serialization.util.GenericSerde;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.common.serialization.Serde;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static io.simplesource.kafka.serialization.avro.AvroSpecificGenericMapper.specificDomainMapper;
import static java.util.Objects.nonNull;


public final class AvroAggregateSerdes<K, C, E, A> implements AggregateSerdes<K, C, E, A> {

    private final Serde<GenericRecord> keySerde;
    private final Serde<GenericRecord> valueSerde;

    private final Serde<K> ak;
    private final Serde<CommandRequest<C>> crq;
    private final Serde<UUID> crk;
    private final Serde<ValueWithSequence<E>> vws;
    private final Serde<AggregateUpdate<A>> au;
    private final Serde<AggregateUpdateResult<A>> aur;
    private final Serde<CommandResponse> crp;

    public static <A extends GenericRecord, E extends GenericRecord, C extends GenericRecord, K extends GenericRecord> AvroAggregateSerdes<A, E, C, K> of(
            final String schemaRegistryUrl,
            final Schema aggregateSchema
    ) {
        return of(
                schemaRegistryUrl,
                false,
                aggregateSchema);
    }

    public static <A extends GenericRecord, E extends GenericRecord, C extends GenericRecord, K extends GenericRecord> AvroAggregateSerdes<A, E, C, K> of(
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
            final GenericMapper<A, GenericRecord> aggregateMapper,
            final GenericMapper<E, GenericRecord> eventMapper,
            final GenericMapper<C, GenericRecord> commandMapper,
            final GenericMapper<K, GenericRecord> keyMapper,
            final String schemaRegistryUrl,
            final boolean useMockSchemaRegistry,
            final Schema aggregateSchema) {

        keySerde = AvroGenericUtils.genericAvroSerde(schemaRegistryUrl, useMockSchemaRegistry, true);
        valueSerde = AvroGenericUtils.genericAvroSerde(schemaRegistryUrl, useMockSchemaRegistry, false);

        ak = GenericSerde.of(keySerde, keyMapper::toGeneric, keyMapper::fromGeneric);
        crq = GenericSerde.of(valueSerde,
                v -> CommandRequestAvroHelper.toGenericRecord(v.map(commandMapper::toGeneric)),
                s -> CommandRequestAvroHelper.fromGenericRecord(s).map(commandMapper::fromGeneric));
        crk = GenericSerde.of(valueSerde,
                CommandResponseKeyAvroHelper::toGenericRecord,
                CommandResponseKeyAvroHelper::fromGenericRecord);
        vws = GenericSerde.of(valueSerde,
                v -> AvroGenericUtils.ValueWithSequenceAvroHelper.toGenericRecord(v.map(eventMapper::toGeneric)),
                s -> AvroGenericUtils.ValueWithSequenceAvroHelper.fromGenericRecord(s).map(eventMapper::fromGeneric));
        au = GenericSerde.of(valueSerde,
                v -> AggregateUpdateAvroHelper.toGenericRecord(v.map(aggregateMapper::toGeneric), aggregateSchema),
                s -> AggregateUpdateAvroHelper.fromGenericRecord(s)
                        .map(aggregateMapper::fromGeneric));
        aur = GenericSerde.of(valueSerde,
                v -> AggregateUpdateResultAvroHelper.toAggregateUpdateResult(v.map(aggregateMapper::toGeneric), aggregateSchema),
                s -> AggregateUpdateResultAvroHelper.fromAggregateUpdateResult(s).map(aggregateMapper::fromGeneric));

        crp = GenericSerde.of(valueSerde,
                v -> CommandResponseAvroHelper.toCommandResponse(v, aggregateSchema),
                CommandResponseAvroHelper::fromCommandResponse);
    }

    @Override
    public Serde<K> aggregateKey() {
        return ak;
    }

    @Override
    public Serde<CommandRequest<C>> commandRequest() {
        return crq;
    }

    @Override
    public Serde<UUID> commandResponseKey() {
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
    public Serde<AggregateUpdateResult<A>> updateResult() {
        return aur;
    }

    @Override
    public Serde<CommandResponse> commandResponse() {
        return crp;
    }

    private static class CommandRequestAvroHelper {
        private static final Map<Schema, Schema> schemaCache = new ConcurrentHashMap<>();

        private static final String READ_SEQUENCE = "readSequence";
        private static final String COMMAND_ID = "commandId";
        private static final String COMMAND = "command";

        static GenericRecord toGenericRecord(
                final CommandRequest<GenericRecord> commandRequest
        ) {
            final GenericRecord command = commandRequest.command();
            final Schema schema = schemaCache.computeIfAbsent(command.getSchema(),
                    k -> commandRequestSchema(command));
            final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
            return builder.set(READ_SEQUENCE, commandRequest.readSequence().getSeq())
                    .set(COMMAND_ID, commandRequest.commandId().toString())
                    .set(COMMAND, command)
                    .build();
        }

        static CommandRequest<GenericRecord> fromGenericRecord(final GenericRecord record) {
            final Sequence readSequence = Sequence.position((Long) record.get(READ_SEQUENCE));
            final UUID commandId = UUID.fromString(String.valueOf(record.get(COMMAND_ID)));
            final GenericRecord command = (GenericRecord) record.get(COMMAND);
            return new CommandRequest<>(command, readSequence, commandId);
        }

        private static Schema commandRequestSchema(final GenericRecord command) {
            return SchemaBuilder
                    .record(command.getSchema().getName() + "CommandRequest").namespace(command.getClass().getPackage().getName())
                    .fields()
                    .name(READ_SEQUENCE).type().longType().noDefault()
                    .name(COMMAND_ID).type().stringType().noDefault()
                    .name(COMMAND).type(command.getSchema()).noDefault()
                    .endRecord();
        }

    }

    private static class CommandResponseKeyAvroHelper {
        private static final Schema schema = commandResponseKeySchema();
        private static final String COMMAND_ID = "commandId";

        static GenericRecord toGenericRecord(
                final UUID commandResponseKey
        ) {
            final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
            return builder
                    .set(COMMAND_ID, commandResponseKey.toString())
                    .build();
        }

        static UUID fromGenericRecord(final GenericRecord record) {
            return UUID.fromString(String.valueOf(record.get(COMMAND_ID)));
        }

        private static Schema commandResponseKeySchema() {
            return SchemaBuilder
                    .record("CommandResponseKey")
                    .namespace("io.simplesource.kafka.serialization.avro")
                    .fields()
                    .name(COMMAND_ID).type().stringType().noDefault()
                    .endRecord();
        }

    }

    private static class AggregateUpdateAvroHelper {
        private static final Map<Schema, Schema> schemaCache = new ConcurrentHashMap<>();

        private static final String AGGREGATION = "aggregate_update";
        private static final String SEQUENCE = "sequence";

        static GenericRecord toGenericRecord(
                final AggregateUpdate<GenericRecord> aggregateUpdate,
                final Schema aggregateSchema
        ) {
            final Schema schema = schemaCache.computeIfAbsent(aggregateSchema,
                    AggregateUpdateAvroHelper::generateSchema);
            final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
            return builder
                    .set(AGGREGATION, aggregateUpdate.aggregate())
                    .set(SEQUENCE, aggregateUpdate.sequence().getSeq())
                    .build();
        }

        static AggregateUpdate<GenericRecord> fromGenericRecord(final GenericRecord record) {
            final GenericRecord genericAggregate = (GenericRecord) record.get(AGGREGATION);
            final Sequence sequence = Sequence.position((Long) record.get(SEQUENCE));

            return new AggregateUpdate<>(genericAggregate, sequence);
        }

        private static Schema generateSchema(final Schema aggregateSchema) {
            return SchemaBuilder
                    .record(aggregateSchema.getName() + "OptionalAggregateWithSequence").namespace(aggregateSchema.getNamespace())
                    .fields()
                    .name(AGGREGATION).type(toNullableSchema(aggregateSchema)).withDefault(null)
                    .name(SEQUENCE).type().longType().noDefault()
                    .endRecord();
        }

    }

    private static class AggregateUpdateResultAvroHelper {
        private static final Map<Schema, Schema> schemaCache = new ConcurrentHashMap<>();

        private static final String READ_SEQUENCE = "readSequence";
        private static final String COMMAND_ID = "commandId";
        private static final String RESULT = "result";
        private static final String REASON = "reason";
        private static final String ADDITIONAL_REASONS = "additionalReasons";
        private static final String ERROR_MESSAGE = "errorMessage";
        private static final String ERROR_CODE = "errorCode";
        private static final String WRITE_SEQUENCE = "writeSequence";
        private static final String AGGREGATION = "aggregate_update";


        static GenericRecord toAggregateUpdateResult(
                final AggregateUpdateResult<GenericRecord> aggregateUpdateResult,
                final Schema aggregateSchema
        ) {
            final Schema schema = schemaCache.computeIfAbsent(aggregateSchema,
                    AggregateUpdateResultAvroHelper::aggregateUpdateResultSchema);
            final Schema resultSchema = schema.getField(RESULT).schema();
            final Schema responseFailureSchema = resultSchema.getTypes().get(0);
            final Schema reasonSchema = responseFailureSchema.getField(REASON).schema();
            final Schema responseSuccessSchema = resultSchema.getTypes().get(1);

            return new GenericRecordBuilder(schema)
                    .set(READ_SEQUENCE, aggregateUpdateResult.readSequence().getSeq())
                    .set(COMMAND_ID, aggregateUpdateResult.commandId().toString())
                    .set(RESULT, aggregateUpdateResult.updatedAggregateResult().fold(
                            reasons -> new GenericRecordBuilder(responseFailureSchema)
                                    .set(REASON, fromReason(reasonSchema, reasons.head()))
                                    .set(ADDITIONAL_REASONS, reasons.tail()
                                            .stream()
                                            .map(reason -> fromReason(reasonSchema, reason))
                                            .collect(Collectors.toList()))
                                    .build(),
                            aggregateUpdate -> new GenericRecordBuilder(responseSuccessSchema)
                                    .set(WRITE_SEQUENCE, aggregateUpdate.sequence().getSeq())
                                    .set(AGGREGATION, aggregateUpdate.aggregate())
                                    .build()
                    ))
                    .build();
        }

        private static GenericRecord fromReason(final Schema schema, final CommandError commandError) {
            return new GenericRecordBuilder(schema)
                    .set(ERROR_MESSAGE, commandError.getMessage())
                    .set(ERROR_CODE, commandError.getReason().name())
                    .build();
        }

        static AggregateUpdateResult<GenericRecord> fromAggregateUpdateResult(
                final GenericRecord record) {
            final Sequence readSequence = Sequence.position((Long) record.get(READ_SEQUENCE));
            final UUID commandId = UUID.fromString(String.valueOf(record.get(COMMAND_ID)));
            final GenericRecord genericResult = (GenericRecord) record.get(RESULT);
            final Result<CommandError, AggregateUpdate<GenericRecord>> result;
            if (nonNull(genericResult.get(WRITE_SEQUENCE))) {
                final Sequence writeSequence = Sequence.position((Long) genericResult.get(WRITE_SEQUENCE));
                final GenericRecord genericAggregate = (GenericRecord) genericResult.get(AGGREGATION);
                result = Result.success(new AggregateUpdate<>(genericAggregate, writeSequence));
            } else {
                final CommandError commandError = toCommandError((GenericRecord) genericResult.get(REASON));
                final List<CommandError> additionalCommandErrors = ((List<GenericRecord>) genericResult.get(ADDITIONAL_REASONS))
                        .stream()
                        .map(AggregateUpdateResultAvroHelper::toCommandError)
                        .collect(Collectors.toList());
                result = Result.failure(new NonEmptyList<>(commandError, additionalCommandErrors));
            }

            return new AggregateUpdateResult<>(commandId, readSequence, result);
        }

        private static CommandError toCommandError(final GenericRecord record) {
            String errorMessage = String.valueOf(record.get(ERROR_MESSAGE));
            final String errorCodeStr = String.valueOf(record.get(ERROR_CODE));
            Reason error;
            try {
                error = CommandError.Reason.valueOf(errorCodeStr);
            } catch (final IllegalArgumentException e) {
                error = CommandError.Reason.UnexpectedErrorCode;
                errorMessage += "Unexpected errorCode " + errorCodeStr;
            }
            return CommandError.of(error, errorMessage);
        }


        private static Schema aggregateUpdateResultSchema(final Schema aggregateSchema) {
            final Schema reasonSchema = SchemaBuilder
                    .record(aggregateSchema.getName() + "Reason")
                    .fields()
                    .name(ERROR_MESSAGE).type().stringType().noDefault()
                    .name(ERROR_CODE).type().stringType().noDefault()
                    .endRecord();
            final Schema updateFailure = SchemaBuilder
                    .record(aggregateSchema.getName() + "CommandResponseFailure")
                    .fields()
                    .name(REASON).type(reasonSchema).noDefault()
                    .name(ADDITIONAL_REASONS).type().array().items(reasonSchema).noDefault()
                    .endRecord();
            final Schema updateSuccess = SchemaBuilder
                    .record(aggregateSchema.getName() + "CommandResponseSuccess")
                    .fields()
                    .name(WRITE_SEQUENCE).type().longType().noDefault()
                    .name(AGGREGATION).type(toNullableSchema(aggregateSchema)).withDefault(null)
                    .endRecord();

            return SchemaBuilder
                    .record(aggregateSchema.getName() + "CommandResponse").namespace(aggregateSchema.getNamespace())
                    .fields()
                    .name(READ_SEQUENCE).type().longType().noDefault()
                    .name(COMMAND_ID).type().stringType().noDefault()
                    .name(RESULT).type(Schema.createUnion(Arrays.asList(updateFailure, updateSuccess))).noDefault()
                    .endRecord();
        }

    }

    private static class CommandResponseAvroHelper {
        private static final Map<Schema, Schema> schemaCache = new ConcurrentHashMap<>();

        private static final String READ_SEQUENCE = "readSequence";
        private static final String COMMAND_ID = "commandId";
        private static final String RESULT = "result";
        private static final String REASON = "reason";
        private static final String ADDITIONAL_REASONS = "additionalReasons";
        private static final String ERROR_MESSAGE = "errorMessage";
        private static final String ERROR_CODE = "errorCode";
        private static final String WRITE_SEQUENCE = "writeSequence";


        static GenericRecord toCommandResponse(
                final CommandResponse commandResponse,
                final Schema aggregateSchema
        ) {
            final Schema schema = schemaCache.computeIfAbsent(aggregateSchema,
                    CommandResponseAvroHelper::commandResponseSchema);
            final Schema resultSchema = schema.getField(RESULT).schema();
            final Schema responseFailureSchema = resultSchema.getTypes().get(0);
            final Schema reasonSchema = responseFailureSchema.getField(REASON).schema();
            final Schema responseSuccessSchema = resultSchema.getTypes().get(1);

            return new GenericRecordBuilder(schema)
                    .set(READ_SEQUENCE, commandResponse.readSequence().getSeq())
                    .set(COMMAND_ID, commandResponse.commandId().toString())
                    .set(RESULT, commandResponse.sequenceResult().fold(
                            reasons -> new GenericRecordBuilder(responseFailureSchema)
                                    .set(REASON, fromReason(reasonSchema, reasons.head()))
                                    .set(ADDITIONAL_REASONS, reasons.tail()
                                            .stream()
                                            .map(reason -> fromReason(reasonSchema, reason))
                                            .collect(Collectors.toList()))
                                    .build(),
                            sequence -> new GenericRecordBuilder(responseSuccessSchema)
                                    .set(WRITE_SEQUENCE, sequence.getSeq())
                                    .build()
                    ))
                    .build();
        }

        private static GenericRecord fromReason(final Schema schema, final CommandError commandError) {
            return new GenericRecordBuilder(schema)
                    .set(ERROR_MESSAGE, commandError.getMessage())
                    .set(ERROR_CODE, commandError.getReason().name())
                    .build();
        }

        static CommandResponse fromCommandResponse(
                final GenericRecord record) {
            final Sequence readSequence = Sequence.position((Long) record.get(READ_SEQUENCE));
            final UUID commandId = UUID.fromString(String.valueOf(record.get(COMMAND_ID)));
            final GenericRecord genericResult = (GenericRecord) record.get(RESULT);
            final Result<CommandError, Sequence> result;
            if (nonNull(genericResult.get(WRITE_SEQUENCE))) {
                final Sequence writeSequence = Sequence.position((Long) genericResult.get(WRITE_SEQUENCE));
                result = Result.success(writeSequence);
            } else {
                final CommandError commandError = toCommandError((GenericRecord) genericResult.get(REASON));
                final List<CommandError> additionalCommandErrors = ((List<GenericRecord>) genericResult.get(ADDITIONAL_REASONS))
                        .stream()
                        .map(AggregateUpdateResultAvroHelper::toCommandError)
                        .collect(Collectors.toList());
                result = Result.failure(new NonEmptyList<>(commandError, additionalCommandErrors));
            }

            return new CommandResponse(commandId, readSequence, result);
        }

        private static CommandError toCommandError(final GenericRecord record) {
            String errorMessage = String.valueOf(record.get(ERROR_MESSAGE));
            final String errorCodeStr = String.valueOf(record.get(ERROR_CODE));
            Reason error;
            try {
                error = CommandError.Reason.valueOf(errorCodeStr);
            } catch (final IllegalArgumentException e) {
                error = CommandError.Reason.UnexpectedErrorCode;
                errorMessage += "Unexpected errorCode " + errorCodeStr;
            }
            return CommandError.of(error, errorMessage);
        }


        private static Schema commandResponseSchema(final Schema aggregateSchema) {
            final Schema reasonSchema = SchemaBuilder
                    .record(aggregateSchema.getName() + "Reason")
                    .fields()
                    .name(ERROR_MESSAGE).type().stringType().noDefault()
                    .name(ERROR_CODE).type().stringType().noDefault()
                    .endRecord();
            final Schema updateFailure = SchemaBuilder
                    .record(aggregateSchema.getName() + "CommandResponseFailure")
                    .fields()
                    .name(REASON).type(reasonSchema).noDefault()
                    .name(ADDITIONAL_REASONS).type().array().items(reasonSchema).noDefault()
                    .endRecord();
            final Schema updateSuccess = SchemaBuilder
                    .record(aggregateSchema.getName() + "CommandResponseSuccess")
                    .fields()
                    .name(WRITE_SEQUENCE).type().longType().noDefault()
                    .endRecord();

            return SchemaBuilder
                    .record(aggregateSchema.getName() + "CommandResponse").namespace(aggregateSchema.getNamespace())
                    .fields()
                    .name(READ_SEQUENCE).type().longType().noDefault()
                    .name(COMMAND_ID).type().stringType().noDefault()
                    .name(RESULT).type(Schema.createUnion(Arrays.asList(updateFailure, updateSuccess))).noDefault()
                    .endRecord();
        }

    }

    /**
     * Return the given schema wrapped in a nullable union.
     */
    private static Schema toNullableSchema(final Schema schema) {
        return Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), schema));
    }

}
