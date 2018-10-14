package io.simplesource.kafka.internal.cluster;

import io.simplesource.api.CommandAPI;

@FunctionalInterface
public interface CommandAPILoader {

    CommandAPI<?> get(String aggregateName);
}
