package io.simplesource.kafka.internal.streams.topologies;

import io.simplesource.kafka.model.AggregateUpdate;
import io.simplesource.kafka.model.AggregateUpdateResult;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import static io.simplesource.kafka.api.AggregateResources.StateStoreEntity.aggregate_update;

final class AggregateStoreUpdateConsumer<K, A> implements AggregateUpdateResultStreamConsumer<K, A> {
    private final AggregateStreamResourceNames aggregateStreamResourceNames;

    AggregateStoreUpdateConsumer(AggregateStreamResourceNames aggregateStreamResourceNames) {
        this.aggregateStreamResourceNames = aggregateStreamResourceNames;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void accept(final KStream<K, AggregateUpdateResult<A>> stream) {
        stream.process(() -> new Processor<K, AggregateUpdateResult<A>>() {
            private KeyValueStore<K, AggregateUpdate<A>> stateStore;

            @Override
            public void init(final ProcessorContext context) {
                stateStore = (KeyValueStore<K, AggregateUpdate<A>>) context.getStateStore(storeName());
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
        }, storeName());
    }

    private String storeName() {
        return aggregateStreamResourceNames.stateStoreName(aggregate_update);
    }
}
