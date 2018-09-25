package io.simplesource.kafka.model;

import io.simplesource.data.Sequence;
import lombok.Value;

import java.util.function.Function;

@Value
public final class AggregateUpdate<A> {
    private final A aggregate;
    private final Sequence sequence;

    public static <A> AggregateUpdate<A> of(A a) {
        return new AggregateUpdate<>(a, Sequence.first());
    }

    public <S> AggregateUpdate<S> map(final Function<A, S> f) {
        return new AggregateUpdate<>(f.apply(aggregate), sequence);
    }
}
