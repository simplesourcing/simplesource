package io.simplesource.kafka.serialization.json;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.simplesource.kafka.serialization.util.GenericMapper;

import java.util.Optional;

import static java.util.Objects.isNull;

public final class JsonOptionalGenericMapper<D> implements GenericMapper<Optional<D>, JsonElement> {

    private static final String VALUE = "value";
    private static final String CLASS = "class";

    private JsonOptionalGenericMapper() {
    }

    private final Gson gson = new Gson();

    @Override
    public JsonElement toGeneric(final Optional<D> value) {
        final JsonObject wrapper = new JsonObject();
        wrapper.add(VALUE, gson.toJsonTree(value.orElse(null)));
        wrapper.addProperty(CLASS, value.map(o -> o.getClass().getName()).orElse(null));
        return wrapper;
    }

    @Override
    public Optional<D> fromGeneric(final JsonElement serialized) {
        final JsonObject wrapper = serialized.getAsJsonObject();
        final JsonElement wrapperClass = wrapper.get(CLASS);
        if (isNull(wrapperClass) || wrapperClass.isJsonNull()) {
            return Optional.empty();
        }
        try {
            final Class<?> clazz = Class.forName(wrapperClass.getAsString());
            return Optional.of((D) gson.fromJson(wrapper.get(VALUE), clazz));
        } catch (final ClassNotFoundException e) {
            throw new IllegalArgumentException("Invalid JSON domain object", e);
        }
    }

    private static final JsonOptionalGenericMapper INSTANCE = new JsonOptionalGenericMapper();

    public static <D> GenericMapper<Optional<D>, JsonElement> jsonOptionalDomainMapper() {
        return INSTANCE;
    }

}
