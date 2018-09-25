package io.simplesource.kafka.serialization.json;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.simplesource.kafka.serialization.util.GenericMapper;

import java.util.Optional;

public final class JsonGenericMapper<D> implements GenericMapper<D, JsonElement> {

    private static final String VALUE = "value";
    private static final String CLASS = "class";

    private JsonGenericMapper() {
    }

    private final Gson gson = new Gson();

    @Override
    public JsonElement toGeneric(final D value) {
        final JsonObject wrapper = new JsonObject();
        wrapper.add(VALUE, gson.toJsonTree(value));
        wrapper.addProperty(CLASS, Optional.ofNullable(value).map(o -> o.getClass().getName()).orElse(null));
        return wrapper;
    }

    @Override
    public D fromGeneric(final JsonElement serialized) {
        final JsonObject wrapper = serialized.getAsJsonObject();
        if (wrapper.get(CLASS).isJsonNull()) {
            return null;
        }
        try {
            final Class<?> clazz = Class.forName(wrapper.get(CLASS).getAsString());
            return (D) gson.fromJson(wrapper.get(VALUE), clazz);
        } catch (final ClassNotFoundException e) {
            throw new IllegalArgumentException("Invalid JSON domain object", e);
        }
    }

    private static final JsonGenericMapper INSTANCE = new JsonGenericMapper();

    public static <D> GenericMapper<D, JsonElement> jsonDomainMapper() {
        return INSTANCE;
    }

}
