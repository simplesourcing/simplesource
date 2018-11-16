package io.simplesource.kafka.internal.client;

import lombok.Value;

import java.util.function.BiFunction;

@Value
public class ResponseReceiver<K, M, V> {
    final ExpiringMap<K, M> expiringMap;
    final BiFunction<M, V, M> mapModifier;

    public void receive(K k, V v) {
        expiringMap.computeIfPresent(k, m -> {
            M updated = mapModifier.apply(m, v);
            return updated;
        });
    }
}
