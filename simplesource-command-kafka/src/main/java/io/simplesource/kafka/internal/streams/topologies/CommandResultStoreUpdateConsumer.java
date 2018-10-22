package io.simplesource.kafka.internal.streams.topologies;

import io.simplesource.kafka.model.AggregateUpdateResult;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;

import java.util.UUID;

import static io.simplesource.kafka.api.AggregateResources.StateStoreEntity.command_response;

final class CommandResultStoreUpdateConsumer<K, A> implements AggregateUpdateResultStreamConsumer<K, A> {
    private final Serde<UUID> keySerde;
    private final Serde<AggregateUpdateResult<A>> aggregateUpdateSerde;
    private final AggregateStreamResourceNames aggregateStreamResourceNames;
    private final Serialized<UUID, AggregateUpdateResult<A>> serialized;
    private final TimeWindows timeWindows;

    CommandResultStoreUpdateConsumer(AggregateStreamResourceNames aggregateStreamResourceNames,
                                     TimeWindows timeWindows, Serde<UUID> keySerde, Serde<AggregateUpdateResult<A>> aggregateUpdateSerde) {
        this.keySerde = keySerde;
        this.aggregateUpdateSerde = aggregateUpdateSerde;
        this.aggregateStreamResourceNames = aggregateStreamResourceNames;
        this.timeWindows = timeWindows;

        this.serialized = Serialized.with(keySerde, aggregateUpdateSerde);
    }

    @Override
    public void accept(KStream<K, AggregateUpdateResult<A>> stream) {
        stream
                .map((k, v) -> KeyValue.pair(v.commandId(), v))
                .groupByKey(serialized)
                .windowedBy(timeWindows)
                .reduce((current, latest) -> latest, materializedWindow(aggregateStreamResourceNames.stateStoreName(command_response)));
    }


    private Materialized<UUID, AggregateUpdateResult<A>, WindowStore<Bytes, byte[]>> materializedWindow(final String storeName) {
        return Materialized
                .<UUID, AggregateUpdateResult<A>, WindowStore<Bytes, byte[]>>as(storeName)
                .withKeySerde(keySerde)
                .withValueSerde(aggregateUpdateSerde);
    }
}
