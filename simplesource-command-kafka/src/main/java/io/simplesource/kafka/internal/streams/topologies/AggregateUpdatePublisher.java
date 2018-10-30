package io.simplesource.kafka.internal.streams.topologies;

import io.simplesource.kafka.api.AggregateResources.StateStoreEntity;
import io.simplesource.kafka.api.AggregateResources.TopicEntity;
import io.simplesource.kafka.model.AggregateUpdate;
import io.simplesource.kafka.model.AggregateUpdateResult;
import io.simplesource.kafka.model.CommandResponse;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

import java.util.UUID;
import java.util.concurrent.TimeUnit;


final class AggregateUpdatePublisher<K, C, E, A> {
    private static final long HOPPING_WINDOW_FACTOR = 3L;
    private final AggregateTopologyContext<K, C, E, A> aggregateTopologyContext;
    private final TimeWindows commandResultTimeWindow;

    AggregateUpdatePublisher(AggregateTopologyContext<K, C, E, A> aggregateTopologyContext) {
        this.aggregateTopologyContext = aggregateTopologyContext;
        final long retentionMillis = TimeUnit.SECONDS.toMillis(aggregateTopologyContext.aggregateSpec().generation().stateStoreSpec().retentionInSeconds());
        commandResultTimeWindow = TimeWindows.of(retentionMillis).advanceBy(retentionMillis / HOPPING_WINDOW_FACTOR);
    }

    @SuppressWarnings("unchecked")
    void toAggregateStore(final KStream<K, AggregateUpdateResult<A>> stream) {
        final String storeName = aggregateTopologyContext.stateStoreName(StateStoreEntity.aggregate_update);
        stream.process(() -> new Processor<K, AggregateUpdateResult<A>>() {
            private KeyValueStore<K, AggregateUpdate<A>> stateStore;

            @Override
            public void init(final ProcessorContext context) {
                stateStore = (KeyValueStore<K, AggregateUpdate<A>>) context.getStateStore(storeName);
            }

            @Override
            public void process(final K readOnlyKey, final AggregateUpdateResult<A> aggregateUpdateResult) {
                aggregateUpdateResult.updatedAggregateResult().ifSuccessful(
                        aggregateUpdate -> stateStore.put(
                                readOnlyKey,
                                aggregateUpdate));
            }

            @Override
            public void close() { }
        }, storeName);
    }

    void toCommandResponseTopic(KStream<K, AggregateUpdateResult<A>> stream) {
        final KStream<K, CommandResponse> aggregateStream = stream
                .mapValues((key, update) ->
                        new CommandResponse(update.commandId(), update.readSequence(),
                                update.updatedAggregateResult().map(AggregateUpdate::sequence))
                );
        aggregateStream.to(aggregateTopologyContext.topicName(TopicEntity.command_response), aggregateTopologyContext.commandResponseProduced());
    }

    void toCommandResultStore(KStream<K, AggregateUpdateResult<A>> stream) {
        stream
                .map((k, v) -> KeyValue.pair(v.commandId(), v))
                .groupByKey(aggregateTopologyContext.serializedAggregateUpdate())
                .windowedBy(commandResultTimeWindow)
                .reduce((current, latest) -> latest, materializedWindow(aggregateTopologyContext.stateStoreName(StateStoreEntity.command_response)));
    }


    private Materialized<UUID, AggregateUpdateResult<A>, WindowStore<Bytes, byte[]>> materializedWindow(final String storeName) {
        return Materialized
                .<UUID, AggregateUpdateResult<A>, WindowStore<Bytes, byte[]>>as(storeName)
                .withKeySerde(aggregateTopologyContext.serdes().commandResponseKey())
                .withValueSerde(aggregateTopologyContext.serdes().updateResult());
    }
}
