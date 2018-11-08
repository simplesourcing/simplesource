package io.simplesource.kafka.internal.streams.topology;

import io.simplesource.kafka.api.AggregateResources;
import io.simplesource.kafka.internal.util.Tuple;
import io.simplesource.kafka.model.*;
import lombok.Value;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Windowed;
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

    <V> V verifyAndReturn(ProducerRecord<K, V> record, boolean isNull, K k, Consumer<V> verifier) {
        if (isNull) {
            assertThat(record).isNull();
            return null;
        }
        if (record == null)
            return null;
        assertThat(record.key()).isEqualTo(k);
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

    List<ValueWithSequence<E>> verifyEvents(K k, Consumer<Tuple<Integer, ValueWithSequence<E>>> verifier) {
        List<ValueWithSequence<E>> eventList = new ArrayList<>();
        int[] index = new int[1];
        while (true) {
            ValueWithSequence<E> response = verifyAndReturn(driver.readOutput(ctx.topicName(TopicEntity.event),
                    ctx.serdes().aggregateKey().deserializer(),
                    ctx.serdes().valueWithSequence().deserializer()), false, k, verifier == null ? null : resp -> verifier.accept(new Tuple<>(index[0], resp)));
            if (response == null) break;
            index[0] = index[0] + 1;
            eventList.add(response);
        }
        return eventList;
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

    WindowStore<UUID, AggregateUpdateResult<A>> getCommandResultStore() {
        WindowStore<UUID, AggregateUpdateResult<A>> store = driver.getWindowStore(ctx.stateStoreName(StateStoreEntity.command_response));
        return store;
    }

    Map<UUID, AggregateUpdateResult<A>> getCommandResults() {
        HashMap<UUID, AggregateUpdateResult<A>> map = new HashMap<>();

        getCommandResultStore().all().forEachRemaining(kv -> {
            Windowed<UUID> wKey = kv.key;
            AggregateUpdateResult<A> update = kv.value;
            map.put(wKey.key(), update);
        });
        return map;
    }


}
