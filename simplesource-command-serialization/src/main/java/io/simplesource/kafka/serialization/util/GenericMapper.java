package io.simplesource.kafka.serialization.util;

import java.util.function.Function;

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

    static <V, S> GenericMapper<V, S> of(Function<V, S> to, Function<S, V> from) {
        return new GenericMapper<V, S>() {
            @Override
            public S toGeneric(V value) {
                return to.apply(value);
            }

            @Override
            public V fromGeneric(S serialized) {
                return from.apply(serialized);
            }
        };
    }
}
