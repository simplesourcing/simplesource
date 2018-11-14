package io.simplesource.kafka.spec;

import lombok.Value;

import java.util.Map;

@Value
public final class CommandSetSpec {
    private final KafkaExecutionSpec executionSpec;
    private final Map<String, CommandSpec<?, ?>> commandConfigMap;
}
