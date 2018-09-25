package io.simplesource.kafka.serialization.util;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;
import java.util.function.Function;

public final class GenericSerde<A, S> implements Serde<A> {

    private final Serde<S> serde;
    private final GenericMapper<A, S> mapper;

    private final Serializer<A> serializer = new Serializer<A>() {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {}

        @Override
        public byte[] serialize(String topic, A data) {
            return serde.serializer().serialize(topic,  mapper.toGeneric(data));
        }

        @Override
        public void close() {}
    };

    private final Deserializer<A> deserializer = new Deserializer<A>() {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {}

        @Override
        public A deserialize(String topic, byte[] data) {
            return mapper.fromGeneric(serde.deserializer().deserialize(topic, data));
        }

        @Override
        public void close() {}
    };

    public static <A, S> Serde<A> of(final Serde<S> serde, final GenericMapper<A, S> mapper) {
        return new GenericSerde<>(serde, mapper);
    }

    public static <A, S> Serde<A> of(final Serde<S> serde,
                                                 final Function<A, S> toGenericFn,
                                                 final Function<S, A> fromGenericFn) {
        GenericMapper<A, S> mapper = new GenericMapper<A, S>() {
            @Override
            public S toGeneric(A value) {
                return toGenericFn.apply(value);
            }

            @Override
            public A fromGeneric(S serialized) {
                return fromGenericFn.apply(serialized);
            }
        };

        return new GenericSerde<>(serde, mapper);
    }

    private GenericSerde(final Serde<S> serde, final GenericMapper<A, S> mapper) {
        this.serde = serde;
        this.mapper = mapper;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        serde.configure(configs, isKey);
    }

    @Override
    public void close() {
        serde.close();
    }

    @Override
    public Serializer<A> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<A> deserializer() {
        return deserializer;
    }
}
