package io.simplesource.kafka.serialization.avro;

import io.simplesource.api.CommandError;
import io.simplesource.api.CommandId;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.data.Sequence;
import io.simplesource.kafka.model.AggregateUpdate;
import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.kafka.model.CommandResponse;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static java.util.Objects.nonNull;

public final class AvroSerdes {

    static class CommandRequestAvroHelper {
        private static final Map<Schema, Schema> schemaCache = new ConcurrentHashMap<>();

        private static final String AGGREGATE_KEY = "key";
        private static final String READ_SEQUENCE = "readSequence";
        private static final String COMMAND_ID = "commandId";
        private static final String COMMAND = "command";

        static GenericRecord toGenericRecord(
                final CommandRequest<GenericRecord, GenericRecord> commandRequest
        ) {
            final GenericRecord command = commandRequest.command();
            final GenericRecord key = commandRequest.aggregateKey();
            final Schema schema = schemaCache.computeIfAbsent(command.getSchema(),
                    k -> commandRequestSchema(command, key));

            final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
            return builder
                    .set(AGGREGATE_KEY, commandRequest.aggregateKey())
                    .set(READ_SEQUENCE, commandRequest.readSequence().getSeq())
                    .set(COMMAND_ID, commandRequest.commandId().id().toString())
                    .set(COMMAND, command)
                    .build();
        }

        static CommandRequest<GenericRecord, GenericRecord> fromGenericRecord(final GenericRecord record) {
            final GenericRecord aggregateKey = (GenericRecord) record.get(AGGREGATE_KEY);
            final Sequence readSequence = Sequence.position((Long) record.get(READ_SEQUENCE));
            final CommandId commandId = CommandId.of(UUID.fromString(String.valueOf(record.get(COMMAND_ID))));
            final GenericRecord command = (GenericRecord) record.get(COMMAND);
            return new CommandRequest<>(commandId, aggregateKey, readSequence, command);
        }

        private static Schema commandRequestSchema(final GenericRecord command, final GenericRecord key) {
            return SchemaBuilder
                    .record(command.getSchema().getName() + "CommandRequest").namespace(command.getClass().getPackage().getName())
                    .fields()
                    .name(AGGREGATE_KEY).type(key.getSchema()).noDefault()
                    .name(READ_SEQUENCE).type().longType().noDefault()
                    .name(COMMAND_ID).type().stringType().noDefault()
                    .name(COMMAND).type(command.getSchema()).noDefault()
                    .endRecord();
        }
    }

    static class CommandResponseKeyAvroHelper {
        private static final Schema schema = commandResponseKeySchema();
        private static final String COMMAND_ID = "commandId";

        static GenericRecord toGenericRecord(
                final CommandId commandResponseKey
        ) {
            final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
            return builder
                    .set(COMMAND_ID, commandResponseKey.id().toString())
                    .build();
        }

        static CommandId fromGenericRecord(final GenericRecord record) {
            return CommandId.of(UUID.fromString(String.valueOf(record.get(COMMAND_ID))));
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

    static class AggregateUpdateResultAvroHelper {
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

        private static GenericRecord fromReason(final Schema schema, final CommandError commandError) {
            return new GenericRecordBuilder(schema)
                    .set(ERROR_MESSAGE, commandError.getMessage())
                    .set(ERROR_CODE, commandError.getReason().name())
                    .build();
        }

        private static CommandError toCommandError(final GenericRecord record) {
            String errorMessage = String.valueOf(record.get(ERROR_MESSAGE));
            final String errorCodeStr = String.valueOf(record.get(ERROR_CODE));
            CommandError.Reason error;
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

    static class CommandResponseAvroHelper {
        private static final Map<Schema, Schema> schemaCache = new ConcurrentHashMap<>();

        private static final String AGGREGATE_KEY = "key";
        private static final String READ_SEQUENCE = "readSequence";
        private static final String COMMAND_ID = "commandId";
        private static final String RESULT = "result";
        private static final String REASON = "reason";
        private static final String ADDITIONAL_REASONS = "additionalReasons";
        private static final String ERROR_MESSAGE = "errorMessage";
        private static final String ERROR_CODE = "errorCode";
        private static final String WRITE_SEQUENCE = "writeSequence";


        static <K> GenericRecord toCommandResponse(
                final CommandResponse<GenericRecord> commandResponse) {
            final GenericRecord key = commandResponse.aggregateKey();
            final Schema schema = commandResponseSchema(key);
            final Schema resultSchema = schema.getField(RESULT).schema();
            final Schema responseFailureSchema = resultSchema.getTypes().get(0);
            final Schema reasonSchema = responseFailureSchema.getField(REASON).schema();
            final Schema responseSuccessSchema = resultSchema.getTypes().get(1);

            return new GenericRecordBuilder(schema)
                    .set(AGGREGATE_KEY, commandResponse.aggregateKey())
                    .set(READ_SEQUENCE, commandResponse.readSequence().getSeq())
                    .set(COMMAND_ID, commandResponse.commandId().id().toString())
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

        static <K> CommandResponse<GenericRecord> fromCommandResponse(
                final GenericRecord record) {
            final GenericRecord aggregateKey = (GenericRecord) record.get(AGGREGATE_KEY);
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

            return new CommandResponse<>(CommandId.of(commandId), aggregateKey, readSequence, result);
        }

        private static CommandError toCommandError(final GenericRecord record) {
            String errorMessage = String.valueOf(record.get(ERROR_MESSAGE));
            final String errorCodeStr = String.valueOf(record.get(ERROR_CODE));
            CommandError.Reason error;
            try {
                error = CommandError.Reason.valueOf(errorCodeStr);
            } catch (final IllegalArgumentException e) {
                error = CommandError.Reason.UnexpectedErrorCode;
                errorMessage += "Unexpected errorCode " + errorCodeStr;
            }
            return CommandError.of(error, errorMessage);
        }

        private static Schema commandResponseSchema(final GenericRecord key) {
            final Schema reasonSchema = SchemaBuilder
                    .record("Reason")
                    .fields()
                    .name(ERROR_MESSAGE).type().stringType().noDefault()
                    .name(ERROR_CODE).type().stringType().noDefault()
                    .endRecord();
            final Schema updateFailure = SchemaBuilder
                    .record( "CommandResponseFailure")
                    .fields()
                    .name(REASON).type(reasonSchema).noDefault()
                    .name(ADDITIONAL_REASONS).type().array().items(reasonSchema).noDefault()
                    .endRecord();
            final Schema updateSuccess = SchemaBuilder
                    .record("CommandResponseSuccess")
                    .fields()
                    .name(WRITE_SEQUENCE).type().longType().noDefault()
                    .endRecord();

            return SchemaBuilder
                    .record("CommandResponse")
                    .namespace("io.simplesource.kafka.serialization.avro")
                    .fields()
                    .name(READ_SEQUENCE).type().longType().noDefault()
                    .name(AGGREGATE_KEY).type(key.getSchema()).noDefault()
                    .name(COMMAND_ID).type().stringType().noDefault()
                    .name(RESULT).type(Schema.createUnion(Arrays.asList(updateFailure, updateSuccess))).noDefault()
                    .endRecord();
        }

    }

    static class AggregateUpdateAvroHelper {
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

    /**
     * Return the given schema wrapped in a nullable union.
     */
    private static Schema toNullableSchema(final Schema schema) {
        return Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), schema));
    }

}
