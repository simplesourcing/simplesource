package io.simplesource.kafka.serialization.util;

/**
 * Provides a mapping from your domain classes for aggregates, events, commands and keys to
 * a generic serialization class used to read and write from Kafka.
 *
 * @param <V> the domain class to read or write to Kafka
 * @param <S> the generic serialization type for all domain objects
 */
public interface GenericMapper<V, S> {
    S toGeneric(V value);
    V fromGeneric(S serialized);
}
