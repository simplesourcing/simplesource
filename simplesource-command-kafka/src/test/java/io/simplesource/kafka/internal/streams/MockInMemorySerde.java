package io.simplesource.kafka.internal.streams;

import io.simplesource.kafka.internal.util.Tuple2;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes.ByteArraySerde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MockInMemorySerde<T> implements Serde<T> {
    private final ByteArraySerde serde = new ByteArraySerde();
    private final static Map<Tuple2<String, Integer>, Object> serialisedObjectCache = new ConcurrentHashMap<>();

    public static void resetCache() {
        serialisedObjectCache.clear();
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
    public Serializer<T> serializer() {
        return new Serializer<T>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {}

            @Override
            public byte[] serialize(String topic, T data) {
                serialisedObjectCache.putIfAbsent(Tuple2.of(topic, data.hashCode()), data);
                return String.valueOf(data.hashCode()).getBytes(Charset.defaultCharset());
            }

            @Override
            public void close() { }
        };
    }

    @Override
    @SuppressWarnings("unchecked")
    public Deserializer<T> deserializer() {
        return new Deserializer<T>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) { }

            @Override
            public T deserialize(String topic, byte[] data) {
                String objectHashcode = new String(data, Charset.defaultCharset());
                return (T) serialisedObjectCache.get(Tuple2.of(topic, Integer.valueOf(objectHashcode)));
            }

            @Override
            public void close() { }
        };
    }
}
