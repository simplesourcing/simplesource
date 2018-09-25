package io.simplesource.kafka.spec;

import lombok.Value;

import java.util.Map;

@Value
public final class AggregateSetSpec {
    private final KafkaExecutionSpec executionSpec;
    private final Map<String, AggregateSpec<?, ?, ?, ?>> aggregateConfigMap;
}
