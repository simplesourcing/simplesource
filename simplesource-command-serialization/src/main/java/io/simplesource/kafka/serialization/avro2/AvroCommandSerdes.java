package io.simplesource.kafka.serialization.avro2;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.simplesource.api.CommandError;
import io.simplesource.api.CommandId;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.data.Sequence;
import io.simplesource.kafka.api.CommandSerdes;
import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.kafka.model.CommandResponse;
import io.simplesource.kafka.serialization.util.SerdeUtils;
import io.simplesource.serialization.avro.generated.*;
import org.apache.avro.generic.GenericArray;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import static io.simplesource.kafka.serialization.avro2.AvroSerdeUtils.*;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class AvroCommandSerdes<K, C> implements CommandSerdes<K, C> {

    private final Serde<K> keySerde;
    private final Serde<C> commandSerde;
    private final Serde<AvroCommandRequest> avroCommandRequestSerde;
    private final Serde<AvroCommandResponse> avroCommandResponseSerde;

    AvroCommandSerdes(
            final Serde<K> keySerde,
            final Serde<C> commandSerde,
            final String schemaRegistryUrl,
            final boolean useMockSchemaRegistry) {
        this.keySerde = keySerde;
        this.commandSerde = commandSerde;

        SchemaRegistryClient regClient = useMockSchemaRegistry ? new MockSchemaRegistryClient() : null;
        avroCommandRequestSerde = SpecificSerdeUtils.specificAvroSerde(schemaRegistryUrl, false, regClient);
        avroCommandResponseSerde = SpecificSerdeUtils.specificAvroSerde(schemaRegistryUrl, false, regClient);
    }

    @Override
    public Serde<CommandId> commandId() {
        return SerdeUtils.iMap(Serdes.UUID(), CommandId::id, CommandId::of);
    }

    @Override
    public Serde<K> aggregateKey() {
        return keySerde;
    }

    @Override
    public Serde<CommandRequest<K, C>> commandRequest() {
        return SerdeUtils.iMap(avroCommandRequestSerde,
                (topic, r) -> commandRequestToAvro(keySerde, commandSerde, topic, r),
                (topic, ar) -> commandRequestFromAvro(keySerde, commandSerde, topic, ar));
    }

    @Override
    public Serde<CommandResponse<K>> commandResponse() {
        return SerdeUtils.iMap(avroCommandResponseSerde,
                (topic, r) -> commandResponseToAvro(keySerde, topic, r),
                (topic, ar) -> commandResponseFromAvro(keySerde, topic, ar));
    }


    static <R, T> Result<CommandError, T> commandResultFromAvro(Object aRes, Function<R, T> successTransformer) {
        // TODO: remove the casting
        Result<CommandError, T> result;
        if (aRes instanceof GenericArray) {
            GenericArray<Object> aResArray = (GenericArray) aRes;
            Stream<AvroCommandError> avroErrors = aResArray.stream()
                    .map(x -> (AvroCommandError) x)
                    .filter(Objects::nonNull);
            result = commandErrorFromAvro(avroErrors);
        } else {
            R tRes = (R) aRes;
            if (tRes != null) {
                result = Result.success(successTransformer.apply(tRes));
            } else {
                result = Result.failure(CommandError.of(CommandError.Reason.InternalError, "Serialization error"));
            }
        }
        return result;
    }

    static <T> Result<CommandError, T> commandErrorFromAvro(Stream<AvroCommandError> aRes) {
        return Result.failure(NonEmptyList.fromList(
                aRes.map(AvroCommandSerdes::commandErrorFromAvro)
                        .collect(Collectors.toList()))
                .orElse(NonEmptyList.of(
                        CommandError.of(CommandError.Reason.InternalError, "Serialization error"))));
    }

    static AvroCommandError commandErrorToAvro(CommandError e) {
        return new AvroCommandError(
                e.getReason().toString(),
                e.getMessage());
    }

    static CommandError commandErrorFromAvro(AvroCommandError e) {
        if (e == null) return null;
        return CommandError.of(
                CommandError.Reason.valueOf(e.getReason()),
                e.getMessage());
    }

    static List<AvroCommandError> commandErrorListToAvro(NonEmptyList<CommandError> es) {
        return es.map(AvroCommandSerdes::commandErrorToAvro).toList();
    }

    static <K> AvroCommandResponse commandResponseToAvro(Serde<K> keySerde, String topicName, CommandResponse<K> response) {
        ByteBuffer serializedKey = AvroSerdeUtils.serializePayload(keySerde, topicName, response.aggregateKey(), PAYLOAD_TYPE_KEY);
        return AvroCommandResponse.newBuilder()
                .setAggregateKey(serializedKey)
                .setCommandId(response.commandId().id.toString())
                .setReadSequence(response.readSequence().getSeq())
                .setResult(response.sequenceResult().fold(AvroCommandSerdes::commandErrorListToAvro, Sequence::getSeq))
                .build();
    }

    static <K> CommandResponse<K> commandResponseFromAvro(Serde<K> keySerde, String topicName, AvroCommandResponse ar) {
        K aggregateKey = AvroSerdeUtils.deserializePayload(keySerde, topicName, ar.getAggregateKey(), AvroSerdeUtils.PAYLOAD_TYPE_KEY);
        Result<CommandError, Sequence> result = commandResultFromAvro(ar.getResult(), Sequence::position);
        return CommandResponse.of(CommandId.fromString(ar.getCommandId()), aggregateKey, Sequence.position(ar.getReadSequence()), result);
    }


    static <K, C> AvroCommandRequest commandRequestToAvro(Serde<K> keySerde, Serde<C> commandSerde, String topicName, CommandRequest<K, C> request) {
        ByteBuffer serializedCommand = AvroSerdeUtils.serializePayload(commandSerde, topicName, request.command(), PAYLOAD_TYPE_COMMAND);
        ByteBuffer serializedKey = AvroSerdeUtils.serializePayload(keySerde, topicName, request.aggregateKey(), PAYLOAD_TYPE_KEY);
        return AvroCommandRequest.newBuilder()
                .setAggregateKey(serializedKey)
                .setCommand(serializedCommand)
                .setCommandId(request.commandId().id.toString())
                .setReadSequence(request.readSequence().getSeq())
                .build();
    }

    static  <K, C> CommandRequest<K, C> commandRequestFromAvro(Serde<K> keySerde, Serde<C> commandSerde, String topicName, AvroCommandRequest ar) {
        C command = AvroSerdeUtils.deserializePayload(commandSerde, topicName, ar.getCommand(), PAYLOAD_TYPE_COMMAND);
        K aggregateKey = AvroSerdeUtils.deserializePayload(keySerde, topicName, ar.getAggregateKey(), PAYLOAD_TYPE_KEY);
        return CommandRequest.of(CommandId.fromString(ar.getCommandId()), aggregateKey, Sequence.position(ar.getReadSequence()), command);
    }

}
