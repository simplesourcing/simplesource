package io.simplesource.kafka.spec;

import io.simplesource.kafka.dsl.KafkaConfig;
import lombok.Value;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

@Value
public final class KafkaExecutionSpec {
    private final ScheduledExecutorService scheduledExecutor;
    private final KafkaConfig kafkaConfig;
}
