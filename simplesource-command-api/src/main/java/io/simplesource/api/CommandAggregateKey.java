package io.simplesource.api;

import io.simplesource.data.Result;

@FunctionalInterface
public interface CommandAggregateKey<K, C> {
    Result<CommandError, K> getAggregateKey(C command);
}
