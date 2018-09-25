package io.simplesource.kafka.api;

public interface Serializer<K, S> {
    K deserialize(S serialized);
    S serialize(K key);
}
