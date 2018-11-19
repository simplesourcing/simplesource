package io.simplesource.kafka.internal.streams.topology;

import io.simplesource.kafka.internal.util.Tuple2;
import io.simplesource.kafka.model.*;
import lombok.Value;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

import java.util.*;
import java.util.function.Consumer;

import static io.simplesource.kafka.api.AggregateResources.TopicEntity;
import static io.simplesource.kafka.api.AggregateResources.StateStoreEntity;
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
        commandPublisher.publish(ctx.topicName(TopicEntity.command_request), key, commandRequest);
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
        return verifyAndReturn(driver.readOutput(ctx.topicName(TopicEntity.event),
                ctx.serdes().aggregateKey().deserializer(),
                ctx.serdes().valueWithSequence().deserializer()), false, k, verifier);
    }

    List<ValueWithSequence<E>> verifyEvents(K k, Consumer<Tuple2<Integer, ValueWithSequence<E>>> verifier) {
        List<ValueWithSequence<E>> eventList = new ArrayList<>();
        int[] index = new int[1];
        while (true) {
            ValueWithSequence<E> response = verifyAndReturn(driver.readOutput(ctx.topicName(TopicEntity.event),
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

    ValueWithSequence<E> verifyNoEvent() {
        return verifyAndReturn(driver.readOutput(ctx.topicName(TopicEntity.event),
                ctx.serdes().aggregateKey().deserializer(),
                ctx.serdes().valueWithSequence().deserializer()), true, null, null);
    }

    CommandResponse verifyCommandResponse(K k, Consumer<CommandResponse> verifier) {
        return verifyAndReturn(driver.readOutput(ctx.topicName(TopicEntity.command_response),
                ctx.serdes().aggregateKey().deserializer(),
                ctx.serdes().commandResponse().deserializer()), false, k, verifier);
    }

    void drainCommandResponses() {
        while (true) {
            if (verifyCommandResponse(null, v -> {}) == null)
                break;
        }
    }

    CommandResponse verifyNoCommandResponse() {
        return verifyAndReturn(driver.readOutput(ctx.topicName(TopicEntity.command_response),
                ctx.serdes().aggregateKey().deserializer(),
                ctx.serdes().commandResponse().deserializer()), true, null, null);
    }

    AggregateUpdate<A> verifyAggregateUpdate(K k, Consumer<AggregateUpdate<A>> verifier) {
        return verifyAndReturn(driver.readOutput(ctx.topicName(TopicEntity.aggregate),
                ctx.serdes().aggregateKey().deserializer(),
                ctx.serdes().aggregateUpdate().deserializer()), false, k, verifier);
    }

    AggregateUpdate<A> verifyNoAggregateUpdate() {
        return verifyAndReturn(driver.readOutput(ctx.topicName(TopicEntity.aggregate),
                ctx.serdes().aggregateKey().deserializer(),
                ctx.serdes().aggregateUpdate().deserializer()), true, null, null);
    }

    void drainAggregateUpdates() {
        while (true) {
            if (verifyAggregateUpdate(null, v -> {}) == null)
                break;
        }
    }

    KeyValueStore<K, AggregateUpdate<A>> getAggegateUpdateStore() {
        KeyValueStore<K, AggregateUpdate<A>> store = driver.getKeyValueStore(ctx.stateStoreName(StateStoreEntity.aggregate_update));
        return store;
    }

    Map<K, AggregateUpdate<A>> getAggegateUpdates() {
        HashMap<K, AggregateUpdate<A>> map = new HashMap<>();

        getAggegateUpdateStore().all().forEachRemaining(kv -> {
            map.put(kv.key, kv.value);
        });
        return map;
    }

    WindowStore<UUID, CommandResponse> getCommandResponseStore() {
        WindowStore<UUID, CommandResponse> store = driver.getWindowStore(ctx.stateStoreName(StateStoreEntity.command_response));
        return store;
    }

    Map<UUID, CommandResponse> getCommandResponses() {
        HashMap<UUID, CommandResponse> map = new HashMap<>();

        getCommandResponseStore().all().forEachRemaining(kv -> {
            Windowed<UUID> wKey = kv.key;
            CommandResponse response = kv.value;
            map.put(wKey.key(), response);
        });
        return map;
    }


}
