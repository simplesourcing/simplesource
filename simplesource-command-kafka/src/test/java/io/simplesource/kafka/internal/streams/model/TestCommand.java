package io.simplesource.kafka.internal.streams.model;

import lombok.Value;

public interface TestCommand {
    @Value
    class CreateCommand implements TestCommand {
        private final String name;
    }
}
