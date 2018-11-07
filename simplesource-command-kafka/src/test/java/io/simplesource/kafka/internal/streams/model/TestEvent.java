package io.simplesource.kafka.internal.streams.model;

import lombok.Value;

public interface TestEvent {
    @Value
    class Created implements TestEvent {
        private final String name;
    }

    @Value
    class Updated implements TestEvent {
        private final String name;
    }

    @Value
    class DoesNothing implements TestEvent {
    }
}
