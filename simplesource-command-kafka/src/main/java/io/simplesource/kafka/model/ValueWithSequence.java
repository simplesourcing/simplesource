package io.simplesource.kafka.model;

import io.simplesource.data.Sequence;
import lombok.Value;

import java.util.function.Function;

@Value
public final class ValueWithSequence<V> {
    private final V value;
    private final Sequence sequence;

    public static <V> ValueWithSequence<V> of(V a) {
        return new ValueWithSequence<>(a, Sequence.first());
    }

    public <S> ValueWithSequence<S> map(final Function<V, S> f) {
        return new ValueWithSequence<>(f.apply(value), sequence);
    }

}
