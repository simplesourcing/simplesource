package io.simplesource.kafka.spec;

import io.simplesource.kafka.dsl.KafkaConfig;
import lombok.Value;

import java.util.Map;

@Value
public final class AggregateSetSpec {
    private final KafkaConfig kafkaConfig;
    private final Map<String, AggregateSpec<?, ?, ?, ?>> aggregateConfigMap;
}

