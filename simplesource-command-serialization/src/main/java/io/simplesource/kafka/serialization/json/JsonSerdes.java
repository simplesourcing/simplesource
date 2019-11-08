package io.simplesource.kafka.serialization.json;

import com.google.gson.*;
import io.simplesource.api.CommandError;
import io.simplesource.api.CommandId;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.data.Sequence;
import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.kafka.model.CommandResponse;
import io.simplesource.kafka.serialization.util.GenericMapper;
import org.apache.commons.lang3.SerializationUtils;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;

class JsonSerdes<K, C> {
    protected final GenericMapper<K, JsonElement> keyMapper;
    protected final GenericMapper<C, JsonElement> commandMapper;

    public JsonSerdes(GenericMapper<K, JsonElement> keyMapper, GenericMapper<C, JsonElement> commandMapper) {
        this.keyMapper = keyMapper;
        this.commandMapper = commandMapper;
    }

    class CommandRequestAdapter implements JsonSerializer<CommandRequest<K, C>>, JsonDeserializer<CommandRequest<K, C>> {

        private static final String AGGREGATE_KEY = "key";
        private static final String READ_SEQUENCE = "readSequence";
        private static final String COMMAND_ID = "commandId";
        private static final String COMMAND = "command";

        @Override
        public JsonElement serialize(
                final CommandRequest<K, C> commandRequest,
                final Type type,
                final JsonSerializationContext jsonSerializationContext
        ) {
            final JsonObject wrapper = new JsonObject();
            wrapper.add(AGGREGATE_KEY, keyMapper.toGeneric(commandRequest.aggregateKey()));
            wrapper.addProperty(READ_SEQUENCE, commandRequest.readSequence().getSeq());
            wrapper.addProperty(COMMAND_ID, commandRequest.commandId().id().toString());
            wrapper.add(COMMAND, commandMapper.toGeneric(commandRequest.command()));
            return wrapper;
        }

        @Override
        public CommandRequest<K, C> deserialize(
                final JsonElement jsonElement, final Type type, final JsonDeserializationContext jsonDeserializationContext
        ) throws JsonParseException {
            final JsonObject wrapper = jsonElement.getAsJsonObject();
            return new CommandRequest<>(
                    CommandId.of(UUID.fromString(wrapper.getAsJsonPrimitive(COMMAND_ID).getAsString())),
                    keyMapper.fromGeneric(wrapper.get(AGGREGATE_KEY)),
                    Sequence.position(wrapper.getAsJsonPrimitive(READ_SEQUENCE).getAsLong()),
                    commandMapper.fromGeneric(wrapper.get(COMMAND)));
        }
    }

    class CommandIdAdapter implements JsonSerializer<CommandId>, JsonDeserializer<CommandId> {

        private static final String COMMAND_ID = "commandId";

        @Override
        public JsonElement serialize(
                final CommandId commandId,
                final Type type,
                final JsonSerializationContext jsonSerializationContext
        ) {
            final JsonObject wrapper = new JsonObject();
            wrapper.addProperty(COMMAND_ID, commandId.id().toString());
            return wrapper;
        }

        @Override
        public CommandId deserialize(
                final JsonElement jsonElement, final Type type, final JsonDeserializationContext jsonDeserializationContext
        ) throws JsonParseException {
            final JsonObject wrapper = jsonElement.getAsJsonObject();
            return CommandId.of(UUID.fromString(wrapper.getAsJsonPrimitive(COMMAND_ID).getAsString()));
        }
    }

    class CommandResponseAdapter implements JsonSerializer<CommandResponse<K>>, JsonDeserializer<CommandResponse> {


        private static final String READ_SEQUENCE = "readSequence";
        private static final String AGGREGATE_KEY = "key";
        private static final String COMMAND_ID = "commandId";
        private static final String RESULT = "result";
        private static final String REASON = "reason";
        private static final String ADDITIONAL_REASONS = "additionalReasons";
        private static final String ERROR_MESSAGE = "errorMessage";
        private static final String ERROR_CLASS = "errorClass";
        private static final String ERROR = "error";
        private static final String WRITE_SEQUENCE = "writeSequence";

        @Override
        public JsonElement serialize(
                final CommandResponse<K> commandResponse,
                final Type type,
                final JsonSerializationContext jsonSerializationContext
        ) {
            final JsonObject wrapper = new JsonObject();
            wrapper.addProperty(READ_SEQUENCE, commandResponse.readSequence().getSeq());
            wrapper.addProperty(COMMAND_ID, commandResponse.commandId().id().toString());
            wrapper.add(AGGREGATE_KEY, keyMapper.toGeneric(commandResponse.aggregateKey()));
            wrapper.add(RESULT, commandResponse.sequenceResult().fold(
                    reasons -> {
                        final JsonObject failureWrapper = new JsonObject();
                        failureWrapper.add(REASON, serializeReason(reasons.head()));
                        final JsonArray additionalReasons = new JsonArray();
                        reasons.tail().forEach(reason -> additionalReasons.add(serializeReason(reason)));
                        failureWrapper.add(ADDITIONAL_REASONS, additionalReasons);
                        return failureWrapper;
                    },
                    sequence -> {
                        final JsonObject successWrapper = new JsonObject();
                        successWrapper.addProperty(WRITE_SEQUENCE, sequence.getSeq());
                        return successWrapper;
                    }
            ));
            return wrapper;
        }

        private JsonElement serializeReason(final CommandError commandError) {
            final JsonObject wrapper = new JsonObject();
            wrapper.addProperty(ERROR_MESSAGE, commandError.getMessage());
            wrapper.addProperty(ERROR_CLASS, commandError.getClass().getCanonicalName());
            wrapper.addProperty(ERROR, Base64.getEncoder().encodeToString(SerializationUtils.serialize(commandError)));
            return wrapper;
        }

        @Override
        public CommandResponse deserialize(
                final JsonElement jsonElement, final Type type, final JsonDeserializationContext jsonDeserializationContext
        ) throws JsonParseException {
            final JsonObject wrapper = jsonElement.getAsJsonObject();
            final Sequence readSequence = Sequence.position(wrapper.getAsJsonPrimitive(READ_SEQUENCE).getAsLong());
            final CommandId commandId = CommandId.of(UUID.fromString(wrapper.getAsJsonPrimitive(COMMAND_ID).getAsString()));
            final JsonObject resultWrapper = wrapper.getAsJsonObject(RESULT);
            final Result result;
            if (resultWrapper.has(REASON)) {
                final CommandError headCommandError = deserializeCommandError(resultWrapper.getAsJsonObject(REASON));
                final List<CommandError> tailCommandErrors = new ArrayList<>();
                resultWrapper.getAsJsonArray(ADDITIONAL_REASONS)
                        .forEach(reason -> tailCommandErrors.add(deserializeCommandError(reason.getAsJsonObject())));
                result = Result.<CommandError, Sequence>failure(new NonEmptyList<>(headCommandError, tailCommandErrors));
            } else {
                result = Result.success(
                        Sequence.position(resultWrapper.getAsJsonPrimitive(WRITE_SEQUENCE).getAsLong()
                        )
                );
            }
            return new CommandResponse<>(
                    commandId,
                    keyMapper.fromGeneric(wrapper.get(AGGREGATE_KEY)),
                    readSequence,
                    result);
        }

        private CommandError deserializeCommandError(final JsonObject element) {
            return SerializationUtils.deserialize(Base64.getDecoder().decode(element.getAsJsonPrimitive(ERROR).getAsString()));
        }
    }

}
