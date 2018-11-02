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
    private final TopologyContext<K, C, E, A> context;

    AggregateUpdatePublisher(TopologyContext<K, C, E, A> context) {
        this.context = context;
    }

    @SuppressWarnings("unchecked")
    void toAggregateStore(final KStream<K, AggregateUpdateResult<A>> stream) {
        final String storeName = context.stateStoreName(StateStoreEntity.aggregate_update);
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
        aggregateStream.to(context.topicName(TopicEntity.command_response), context.commandResponseProduced());
    }

    void toCommandResultStore(KStream<K, AggregateUpdateResult<A>> stream) {
        final long retentionMillis = TimeUnit.SECONDS.toMillis(context.aggregateSpec().generation().stateStoreSpec().retentionInSeconds());
        TimeWindows         commandResultTimeWindow = TimeWindows.of(retentionMillis).advanceBy(retentionMillis / HOPPING_WINDOW_FACTOR);
        stream
                .map((k, v) -> KeyValue.pair(v.commandId(), v))
                .groupByKey(context.serializedAggregateUpdate())
                .windowedBy(commandResultTimeWindow)
                .reduce((current, latest) -> latest, materializedWindow(context.stateStoreName(StateStoreEntity.command_response)));
    }


    private Materialized<UUID, AggregateUpdateResult<A>, WindowStore<Bytes, byte[]>> materializedWindow(final String storeName) {
        return Materialized
                .<UUID, AggregateUpdateResult<A>, WindowStore<Bytes, byte[]>>as(storeName)
                .withKeySerde(context.serdes().commandResponseKey())
                .withValueSerde(context.serdes().updateResult());
    }
}
