package io.simplesource.kafka.internal.client;

import io.simplesource.kafka.dsl.KafkaConfig;
import io.simplesource.kafka.spec.TopicSpec;
import io.simplesource.kafka.spec.WindowSpec;
import lombok.Builder;
import lombok.Value;
import org.apache.kafka.common.serialization.Serde;

import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;

@Value
@Builder
public final class RequestAPIContext<K, I, O> {
    final KafkaConfig kafkaConfig;
    final ScheduledExecutorService scheduler;
    final String requestTopic;
    final String responseTopicMapTopic;
    final String privateResponseTopic;
    final Serde<K> requestKeySerde;
    final Serde<I> requestValueSerde;
    final Serde<UUID> responseKeySerde;
    final Serde<O> responseValueSerde;
    final WindowSpec responseWindowSpec;
    final TopicSpec outputTopicConfig;
    final BiFunction<I, Throwable, O> errorValue;
}
