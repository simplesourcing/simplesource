package io.simplesource.kafka.internal.streams.topology;

import io.simplesource.kafka.internal.util.Tuple2;
import io.simplesource.kafka.model.AggregateUpdate;
import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.kafka.model.CommandResponse;
import io.simplesource.kafka.model.ValueWithSequence;
import lombok.Value;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.TopologyTestDriver;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static io.simplesource.kafka.api.AggregateResources.TopicEntity;
import static org.assertj.core.api.Assertions.assertThat;

@Value
class TestContextDriver<K, C, E, A> {
    final TopologyContext<K, C, E, A> ctx;
    final TopologyTestDriver driver;
    private TestDriverPublisher<K, CommandRequest<K, C>> commandPublisher;

    TestContextDriver(final TopologyContext<K, C, E, A> ctx, final TopologyTestDriver driver) {
        this.driver = driver;
        this.ctx = ctx;
        commandPublisher = new TestDriverPublisher<>(driver, ctx.serdes().aggregateKey(), ctx.serdes().commandRequest());
    }

    void publishCommand(K key, CommandRequest<K, C> commandRequest) {
        commandPublisher.publish(ctx.topicName(TopicEntity.COMMAND_REQUEST), key, commandRequest);
    }

    public <KP, VP> TestDriverPublisher<KP, VP> getPublisher(final Serde<KP> keySerde, final Serde<VP> valueSerde) {
        return new TestDriverPublisher<>(driver, keySerde, valueSerde);
    }

    <V> V verifyAndReturn(ProducerRecord<K, V> record, boolean isNull, K k, Consumer<V> verifier) {
        if (isNull) {
            assertThat(record).isNull();
            return null;
        }
        if (record == null)
            return null;
        if (k != null) {
            assertThat(record.key()).isEqualTo(k);
        }
        if (verifier != null) {
            verifier.accept(record.value());
        }
        return record.value();
    }

    ValueWithSequence<E> verifyEvent(K k, Consumer<ValueWithSequence<E>> verifier) {
        return verifyAndReturn(driver.readOutput(ctx.topicName(TopicEntity.EVENT),
                ctx.serdes().aggregateKey().deserializer(),
                ctx.serdes().valueWithSequence().deserializer()), false, k, verifier);
    }

    List<ValueWithSequence<E>> verifyEvents(K k, Consumer<Tuple2<Integer, ValueWithSequence<E>>> verifier) {
        List<ValueWithSequence<E>> eventList = new ArrayList<>();
        int[] index = new int[1];
        while (true) {
            ValueWithSequence<E> response = verifyAndReturn(driver.readOutput(ctx.topicName(TopicEntity.EVENT),
                    ctx.serdes().aggregateKey().deserializer(),
                    ctx.serdes().valueWithSequence().deserializer()), false, k, verifier == null ? null : resp -> verifier.accept(new Tuple2<>(index[0], resp)));
            if (response == null) break;
            index[0] = index[0] + 1;
            eventList.add(response);
        }
        return eventList;
    }

    void drainEvents() {
        while (true) {
            if (verifyEvent(null, v -> {}) == null)
                break;
        }
    }

    void verifyNoEvent() {
        verifyAndReturn(driver.readOutput(ctx.topicName(TopicEntity.EVENT),
                ctx.serdes().aggregateKey().deserializer(),
                ctx.serdes().valueWithSequence().deserializer()), true, null, null);
    }

    CommandResponse verifyCommandResponse(K k, Consumer<CommandResponse<K>> verifier) {
        return verifyAndReturn(driver.readOutput(ctx.topicName(TopicEntity.COMMAND_RESPONSE),
                ctx.serdes().aggregateKey().deserializer(),
                ctx.serdes().commandResponse().deserializer()), false, k, verifier);
    }

    void drainCommandResponses() {
        while (true) {
            if (verifyCommandResponse(null, v -> {}) == null)
                break;
        }
    }

    void verifyNoCommandResponse() {
        verifyAndReturn(driver.readOutput(ctx.topicName(TopicEntity.COMMAND_RESPONSE),
                ctx.serdes().aggregateKey().deserializer(),
                ctx.serdes().commandResponse().deserializer()), true, null, null);
    }

    AggregateUpdate<A> verifyAggregateUpdate(K k, Consumer<AggregateUpdate<A>> verifier) {
        return verifyAndReturn(driver.readOutput(ctx.topicName(TopicEntity.AGGREGATE),
                ctx.serdes().aggregateKey().deserializer(),
                ctx.serdes().aggregateUpdate().deserializer()), false, k, verifier);
    }

    AggregateUpdate<A> verifyNoAggregateUpdate() {
        return verifyAndReturn(driver.readOutput(ctx.topicName(TopicEntity.AGGREGATE),
                ctx.serdes().aggregateKey().deserializer(),
                ctx.serdes().aggregateUpdate().deserializer()), true, null, null);
    }

    void drainAggregateUpdates() {
        while (true) {
            if (verifyAggregateUpdate(null, v -> {}) == null)
                break;
        }
    }
}
