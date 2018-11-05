package io.simplesource.kafka.internal.streams.topology;

import io.simplesource.kafka.model.AggregateUpdate;
import io.simplesource.kafka.model.AggregateUpdateResult;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.simplesource.kafka.api.AggregateResources.StateStoreEntity.aggregate_update;
import static io.simplesource.kafka.api.AggregateResources.StateStoreEntity.command_response;
import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;

final class EventSourcedStores {

    static <K, C, E, A> void addStateStores(TopologyContext<K, C, E, A> ctx, final StreamsBuilder builder) {

        final KeyValueStoreBuilder<K, AggregateUpdate<A>> aggregateStoreBuilder = new KeyValueStoreBuilder<>(
                persistentKeyValueStore(ctx.stateStoreName(aggregate_update)),
                ctx.serdes().aggregateKey(),
                ctx.serdes().aggregateUpdate(),
                Time.SYSTEM);
        builder.addStateStore(aggregateStoreBuilder);
    }

    /**
     * Update the state store with the latest aggregate_update value on successful updates.
     */
    static <K, A> void updateAggregateStateStore(TopologyContext<K, ?, ?, A> ctx, final KStream<K, AggregateUpdateResult<A>> aggregateUpdateStream) {
        aggregateUpdateStream.process(() -> new Processor<K, AggregateUpdateResult<A>>() {
            private KeyValueStore<K, AggregateUpdate<A>> stateStore;

            @Override
            public void init(final ProcessorContext context) {
                stateStore = (KeyValueStore<K, AggregateUpdate<A>>) context.getStateStore(ctx.stateStoreName(aggregate_update));
            }

            @Override
            public void process(final K readOnlyKey, final AggregateUpdateResult<A> aggregateUpdateResult) {
                aggregateUpdateResult.updatedAggregateResult().ifSuccessful(
                        aggregateUpdate -> stateStore.put(
                                readOnlyKey,
                                aggregateUpdate));
            }

            @Override
            public void close() {
            }
        }, ctx.stateStoreName(aggregate_update));
    }

    static <K, A> void updateCommandResultStore(TopologyContext<K, ?, ?, A> ctx, final KStream<K, AggregateUpdateResult<A>> aggregateUpdateStream) {
        final long retentionMillis = TimeUnit.SECONDS.toMillis(ctx.commandResponseRetentionInSeconds());
        aggregateUpdateStream
                .map((k, v) -> KeyValue.pair(
                        v.commandId(),
                        v))
                .groupByKey(ctx.serializedAggregateUpdate())
                .windowedBy(
                        TimeWindows
                                .of(retentionMillis)
                                .advanceBy(retentionMillis / 3L))
                .reduce((current, latest) -> latest, materializedWindow(ctx, ctx.stateStoreName(command_response)));
    }

    private static <A> Materialized<UUID, AggregateUpdateResult<A>, WindowStore<Bytes, byte[]>> materializedWindow(TopologyContext<?, ?, ?, A> ctx, final String storeName) {
        return Materialized
                .<UUID, AggregateUpdateResult<A>, WindowStore<Bytes, byte[]>>as(storeName)
                .withKeySerde(ctx.serdes().commandResponseKey())
                .withValueSerde(ctx.serdes().updateResult());
    }
}
