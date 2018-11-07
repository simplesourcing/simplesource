package io.simplesource.kafka.internal.streams.model;

import lombok.Value;

public interface TestCommand {
    @Value
    class CreateCommand implements TestCommand {
        private final String name;
    }

    @Value
    class UpdateCommand implements TestCommand {
        private final String name;
    }

    @Value
    class UpdateWithNothingCommand implements TestCommand {
        private final String name;
    }

    @Value
    class UnsupportedCommand implements TestCommand {
    }
}
