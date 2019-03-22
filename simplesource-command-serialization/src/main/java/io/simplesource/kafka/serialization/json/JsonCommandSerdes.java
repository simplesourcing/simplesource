package io.simplesource.kafka.serialization.json;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import io.simplesource.api.CommandError;
import io.simplesource.api.CommandError.Reason;
import io.simplesource.api.CommandId;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.data.Sequence;
import io.simplesource.kafka.api.AggregateSerdes;
import io.simplesource.kafka.api.CommandSerdes;
import io.simplesource.kafka.model.*;
import io.simplesource.kafka.serialization.util.GenericMapper;
import io.simplesource.kafka.serialization.util.GenericSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static io.simplesource.kafka.serialization.json.JsonGenericMapper.jsonDomainMapper;

public final class JsonCommandSerdes<K, C> extends JsonSerdes<K, C> implements CommandSerdes<K, C> {

    private final Serde<String> serde;
    private final Gson gson;
    private final JsonParser parser;

    private final Serde<K> ak;
    private final Serde<CommandRequest<K, C>> cr;
    private final Serde<CommandId> crk;
    private final Serde<CommandResponse<K>> cr2;

    public JsonCommandSerdes() {
        this(jsonDomainMapper(), jsonDomainMapper());
    }

    public JsonCommandSerdes(
            final GenericMapper<K, JsonElement> keyMapper,
            final GenericMapper<C, JsonElement> commandMapper) {

        super(keyMapper, commandMapper);
        serde = Serdes.String();

        final GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(CommandRequest.class, new CommandRequestAdapter());
        gsonBuilder.registerTypeAdapter(CommandId.class, new CommandIdAdapter());
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
                s -> gson.fromJson(s, new TypeToken<CommandId>() {
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
    public Serde<CommandId> commandResponseKey() {
        return crk;
    }

    @Override
    public Serde<CommandResponse<K>> commandResponse() {
        return cr2;
    }
}
