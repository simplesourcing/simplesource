package io.simplesource.kafka.internal.streams.statestore;

import io.simplesource.kafka.api.AggregateSerdes;
import io.simplesource.kafka.model.AggregateUpdate;
import io.simplesource.kafka.spec.AggregateSpec;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.*;

import java.util.Optional;

import static io.simplesource.kafka.api.AggregateResources.StateStoreEntity.aggregate_update;

public final class KafkaStreamAggregateStoreBridge<K, A> implements AggregateStoreBridge<K, A> {

    private final KafkaStreams kafkaStreams;
    private final AggregateSerdes<K, ?, ?, ?> aggregateSerdes;
    private final String aggregateStoreName;

    public KafkaStreamAggregateStoreBridge(
        final AggregateSpec<K, ?, ?, ?> aggregateSpec,
        final KafkaStreams kafkaStreams) {
        this.kafkaStreams = kafkaStreams;
        aggregateSerdes = aggregateSpec.serialization().serdes();
        aggregateStoreName = aggregateSpec
            .serialization()
            .resourceNamingStrategy()
            .storeName(aggregateSpec.aggregateName(), aggregate_update.name());
    }

    @Override
    public ReadOnlyKeyValueStore<K, AggregateUpdate<A>> getAggregateStateStore() {
        return kafkaStreams.store(aggregateStoreName, QueryableStoreTypes.keyValueStore());
    }

    @Override
    public Optional<HostInfo> hostInfoForAggregateStoreKey(final K key) {
        return Optional
            .ofNullable(kafkaStreams.metadataForKey(
                aggregateStoreName,
                key,
                aggregateSerdes.aggregateKey().serializer()))
            .map(StreamsMetadata::hostInfo);
    }

}
