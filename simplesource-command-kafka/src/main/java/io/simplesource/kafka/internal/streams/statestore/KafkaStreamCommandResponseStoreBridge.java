package io.simplesource.kafka.internal.streams.statestore;

import io.simplesource.kafka.api.AggregateSerdes;
import io.simplesource.kafka.model.AggregateUpdateResult;
import io.simplesource.kafka.spec.AggregateSpec;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.StreamsMetadata;

import java.util.Optional;
import java.util.UUID;

import static io.simplesource.kafka.api.AggregateResources.StateStoreEntity.command_response;


public final class KafkaStreamCommandResponseStoreBridge<K, A> implements CommandResponseStoreBridge<A> {

    private final KafkaStreams kafkaStreams;
    private final AggregateSerdes<K, ?, ?, ?> aggregateSerdes;
    private final String commandResponseStoreName;

    public KafkaStreamCommandResponseStoreBridge(
        final AggregateSpec<K, ?, ?, ?> aggregateSpec,
        final KafkaStreams kafkaStreams) {
        this.kafkaStreams = kafkaStreams;
        aggregateSerdes = aggregateSpec.serialization().serdes();
        commandResponseStoreName = aggregateSpec
            .serialization()
            .resourceNamingStrategy()
            .storeName(aggregateSpec.aggregateName(), command_response.name());
    }

    @Override
    public ReadOnlyWindowStore<UUID, AggregateUpdateResult<A>> getCommandResponseStore() {
        return kafkaStreams.store(commandResponseStoreName, QueryableStoreTypes.windowStore());
    }

    @Override
    public Optional<HostInfo> hostInfoForCommandResponseStoreKey(final UUID key) {
        return Optional
            .ofNullable(kafkaStreams.metadataForKey(
                commandResponseStoreName,
                key,
                aggregateSerdes.commandKey().serializer()))
            .map(StreamsMetadata::hostInfo);
    }
}
