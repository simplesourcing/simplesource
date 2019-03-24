package io.simplesource.kafka.serialization.json;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import io.simplesource.api.CommandId;
import io.simplesource.kafka.api.CommandSerdes;
import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.kafka.model.CommandResponse;
import io.simplesource.kafka.serialization.util.GenericMapper;
import io.simplesource.kafka.serialization.util.GenericSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

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
    public Serde<CommandId> commandId() {
        return crk;
    }

    @Override
    public Serde<CommandResponse<K>> commandResponse() {
        return cr2;
    }
}
