package io.simplesource.kafka.internal.util;

import lombok.Value;

@Value
public final class Tuple<V1, V2> {
    private final V1 v1;
    private final V2 v2;

    public static <V1, V2> Tuple<V1, V2> of(V1 v1, V2 v2) {
        return new Tuple<>(v1, v2);
    }
}
