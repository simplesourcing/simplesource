package io.simplesource.kafka.serialization.json;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import io.simplesource.api.CommandError.Reason;
import io.simplesource.api.CommandError;
import io.simplesource.data.Sequence;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.kafka.api.AggregateSerdes;
import io.simplesource.kafka.serialization.util.GenericMapper;
import io.simplesource.kafka.model.*;
import io.simplesource.kafka.serialization.util.GenericSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.lang.reflect.Type;
import java.util.*;

import static io.simplesource.kafka.serialization.json.JsonGenericMapper.jsonDomainMapper;

public final class JsonAggregateSerdes<K, C, E, A> extends JsonSerdes<K, C> implements AggregateSerdes<K, C, E, A> {

    private final GenericMapper<A, JsonElement> aggregateMapper;
    private final GenericMapper<E, JsonElement> eventMapper;
    private final Serde<String> serde;
    private final Gson gson;
    private final JsonParser parser;

    private final Serde<K> ak;
    private final Serde<CommandRequest<K, C>> cr;
    private final Serde<UUID> crk;
    private final Serde<ValueWithSequence<E>> vws;
    private final Serde<AggregateUpdate<A>> au;
    private final Serde<CommandResponse> cr2;

    public JsonAggregateSerdes() {
        this(jsonDomainMapper(), jsonDomainMapper(), jsonDomainMapper(), jsonDomainMapper());
    }

    public JsonAggregateSerdes(
            final GenericMapper<K, JsonElement> keyMapper,
            final GenericMapper<C, JsonElement> commandMapper,
            final GenericMapper<E, JsonElement> eventMapper,
            final GenericMapper<A, JsonElement> aggregateMapper
            ) {
        super(keyMapper, commandMapper);
        this.aggregateMapper = aggregateMapper;
        this.eventMapper = eventMapper;
        serde = Serdes.String();

        final GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(CommandRequest.class, new CommandRequestAdapter());
        gsonBuilder.registerTypeAdapter(UUID.class, new UUIDAdapter());
        gsonBuilder.registerTypeAdapter(ValueWithSequence.class, new ValueWithSequenceAdapter());
        gsonBuilder.registerTypeAdapter(AggregateUpdate.class, new AggregateUpdateAdapter());
        gsonBuilder.registerTypeAdapter(CommandResponse.class, new CommandResponseAdapter());
        gson = gsonBuilder.create();
        parser = new JsonParser();

        ak = GenericSerde.of(serde,
                k -> keyMapper.toGeneric(k).toString(),
                s -> keyMapper.fromGeneric(parser.parse(s)));
        cr = GenericSerde.of(serde,
                gson::toJson,
                s -> gson.fromJson(s, new TypeToken<CommandRequest<K, C>>() {
                }.getType()));
        crk = GenericSerde.of(serde,
                gson::toJson,
                s -> gson.fromJson(s, new TypeToken<UUID>() {
                }.getType()));
        vws = GenericSerde.of(serde,
                gson::toJson,
                s -> gson.fromJson(s, new TypeToken<ValueWithSequence<E>>() {
                }.getType()));
        au = GenericSerde.of(serde,
                gson::toJson,
                s -> gson.fromJson(s, new TypeToken<AggregateUpdate<A>>() {
                }.getType()));
        cr2 = GenericSerde.of(serde,
                gson::toJson,
                s -> gson.fromJson(s, new TypeToken<CommandResponse>() {
                }.getType()));
    }

    @Override
    public Serde<K> aggregateKey() {
        return ak;
    }

    @Override
    public Serde<CommandRequest<K, C>> commandRequest() {
        return cr;
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
    public Serde<CommandResponse> commandResponse() {
        return cr2;
    }

    private class CommandRequestAdapter implements JsonSerializer<CommandRequest<K, C>>, JsonDeserializer<CommandRequest<K, C>> {

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
            wrapper.addProperty(COMMAND_ID, commandRequest.commandId().toString());
            wrapper.add(COMMAND, commandMapper.toGeneric(commandRequest.command()));
            return wrapper;
        }

        @Override
        public CommandRequest<K, C> deserialize(
                final JsonElement jsonElement, final Type type, final JsonDeserializationContext jsonDeserializationContext
        ) throws JsonParseException {
            final JsonObject wrapper = jsonElement.getAsJsonObject();
            return new CommandRequest<>(
                    keyMapper.fromGeneric(wrapper.get(AGGREGATE_KEY)),
                    commandMapper.fromGeneric(wrapper.get(COMMAND)),
                    Sequence.position(wrapper.getAsJsonPrimitive(READ_SEQUENCE).getAsLong()),
                    UUID.fromString(wrapper.getAsJsonPrimitive(COMMAND_ID).getAsString()));
        }
    }

    private class UUIDAdapter implements JsonSerializer<UUID>, JsonDeserializer<UUID> {

        private static final String COMMAND_ID = "commandId";

        @Override
        public JsonElement serialize(
                final UUID uuid,
                final Type type,
                final JsonSerializationContext jsonSerializationContext
        ) {
            final JsonObject wrapper = new JsonObject();
            wrapper.addProperty(COMMAND_ID, uuid.toString());
            return wrapper;
        }

        @Override
        public UUID deserialize(
                final JsonElement jsonElement, final Type type, final JsonDeserializationContext jsonDeserializationContext
        ) throws JsonParseException {
            final JsonObject wrapper = jsonElement.getAsJsonObject();
            return UUID.fromString(wrapper.getAsJsonPrimitive(COMMAND_ID).getAsString());
        }
    }

    private class ValueWithSequenceAdapter implements JsonSerializer<ValueWithSequence<E>>, JsonDeserializer<ValueWithSequence<E>> {

        private static final String VALUE = "value";
        private static final String SEQUENCE = "sequence";

        @Override
        public JsonElement serialize(
                final ValueWithSequence<E> valueWithSequence,
                final Type type,
                final JsonSerializationContext jsonSerializationContext
        ) {
            final JsonObject wrapper = new JsonObject();
            wrapper.add(VALUE, eventMapper.toGeneric(valueWithSequence.value()));
            wrapper.addProperty(SEQUENCE, valueWithSequence.sequence().getSeq());
            return wrapper;
        }

        @Override
        public ValueWithSequence<E> deserialize(
                final JsonElement jsonElement, final Type type, final JsonDeserializationContext jsonDeserializationContext
        ) throws JsonParseException {
            final JsonObject wrapper = jsonElement.getAsJsonObject();
            return new ValueWithSequence<>(
                    eventMapper.fromGeneric(wrapper.get(VALUE)),
                    Sequence.position(wrapper.getAsJsonPrimitive(SEQUENCE).getAsLong()));
        }
    }

    private class AggregateUpdateAdapter implements JsonSerializer<AggregateUpdate<A>>, JsonDeserializer<AggregateUpdate<A>> {

        private static final String AGGREGATION = "aggregate_update";
        private static final String SEQUENCE = "sequence";

        @Override
        public JsonElement serialize(
                final AggregateUpdate<A> aggregateUpdate,
                final Type type,
                final JsonSerializationContext jsonSerializationContext
        ) {
            final JsonObject wrapper = new JsonObject();
            wrapper.add(AGGREGATION, aggregateMapper.toGeneric(aggregateUpdate.aggregate()));
            wrapper.addProperty(SEQUENCE, aggregateUpdate.sequence().getSeq());
            return wrapper;
        }

        @Override
        public AggregateUpdate<A> deserialize(
                final JsonElement jsonElement, final Type type, final JsonDeserializationContext jsonDeserializationContext
        ) throws JsonParseException {
            final JsonObject wrapper = jsonElement.getAsJsonObject();
            return new AggregateUpdate<>(
                    aggregateMapper.fromGeneric(wrapper.get(AGGREGATION)),
                    Sequence.position(wrapper.getAsJsonPrimitive(SEQUENCE).getAsLong()));
        }
    }

    private class CommandResponseAdapter implements JsonSerializer<CommandResponse>, JsonDeserializer<CommandResponse> {


        private static final String READ_SEQUENCE = "readSequence";
        private static final String COMMAND_ID = "commandId";
        private static final String RESULT = "result";
        private static final String REASON = "reason";
        private static final String ADDITIONAL_REASONS = "additionalReasons";
        private static final String ERROR_MESSAGE = "errorMessage";
        private static final String ERROR_CODE = "errorCode";
        private static final String WRITE_SEQUENCE = "writeSequence";

        @Override
        public JsonElement serialize(
                final CommandResponse commandResponse,
                final Type type,
                final JsonSerializationContext jsonSerializationContext
        ) {
            final JsonObject wrapper = new JsonObject();
            wrapper.addProperty(READ_SEQUENCE, commandResponse.readSequence().getSeq());
            wrapper.addProperty(COMMAND_ID, commandResponse.commandId().toString());
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
            wrapper.addProperty(ERROR_CODE, commandError.getReason().name());
            return wrapper;
        }

        @Override
        public CommandResponse deserialize(
                final JsonElement jsonElement, final Type type, final JsonDeserializationContext jsonDeserializationContext
        ) throws JsonParseException {
            final JsonObject wrapper = jsonElement.getAsJsonObject();
            final Sequence readSequence = Sequence.position(wrapper.getAsJsonPrimitive(READ_SEQUENCE).getAsLong());
            final UUID commandId = UUID.fromString(wrapper.getAsJsonPrimitive(COMMAND_ID).getAsString());
            final JsonObject resultWrapper = wrapper.getAsJsonObject(RESULT);
            final Result result;
            if (resultWrapper.has(REASON)) {
                final CommandError headCommandError = deserializeReason(resultWrapper.getAsJsonObject(REASON));
                final List<CommandError> tailCommandErrors = new ArrayList<>();
                resultWrapper.getAsJsonArray(ADDITIONAL_REASONS)
                        .forEach(reason -> tailCommandErrors.add(deserializeReason(reason.getAsJsonObject())));
                result = Result.<CommandError, Sequence>failure(new NonEmptyList(headCommandError, tailCommandErrors));
            } else {
                result = Result.success(
                        Sequence.position(resultWrapper.getAsJsonPrimitive(WRITE_SEQUENCE).getAsLong()
                        )
                );
            }
            return new CommandResponse(
                    commandId,
                    readSequence,
                    result);
        }

        private CommandError deserializeReason(final JsonObject element) {
            return CommandError.of(
                    Reason.valueOf(element.getAsJsonPrimitive(ERROR_CODE).getAsString()),
                    element.getAsJsonPrimitive(ERROR_MESSAGE).getAsString());
        }
    }
}
