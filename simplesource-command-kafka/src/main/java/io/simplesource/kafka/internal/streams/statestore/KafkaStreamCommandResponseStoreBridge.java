package io.simplesource.kafka.internal.streams.statestore;

import io.simplesource.kafka.api.AggregateSerdes;
import io.simplesource.kafka.api.CommandSerdes;
import io.simplesource.kafka.model.AggregateUpdateResult;
import io.simplesource.kafka.model.CommandResponse;
import io.simplesource.kafka.spec.AggregateSpec;
import io.simplesource.kafka.spec.CommandSpec;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.StreamsMetadata;

import java.util.Optional;
import java.util.UUID;

import static io.simplesource.kafka.api.AggregateResources.StateStoreEntity.command_response;


public final class KafkaStreamCommandResponseStoreBridge<K> implements CommandResponseStoreBridge {

    private final KafkaStreams kafkaStreams;
    private final CommandSerdes<K, ?> commandSerdes;
    private final String commandResponseStoreName;

    public KafkaStreamCommandResponseStoreBridge(
        final CommandSpec<K, ?> commandSpec,
        final KafkaStreams kafkaStreams) {
        this.kafkaStreams = kafkaStreams;
        this.commandSerdes = commandSpec.serdes();
        commandResponseStoreName = commandSpec
            .resourceNamingStrategy()
            .storeName(commandSpec.aggregateName(), command_response.name());
    }

    @Override
    public ReadOnlyWindowStore<UUID, CommandResponse> getCommandResponseStore() {
        return kafkaStreams.store(commandResponseStoreName, QueryableStoreTypes.windowStore());
    }

    @Override
    public Optional<HostInfo> hostInfoForCommandResponseStoreKey(final UUID key) {
        return Optional
            .ofNullable(kafkaStreams.metadataForKey(
                commandResponseStoreName,
                key, commandSerdes.commandResponseKey().serializer()))
            .map(StreamsMetadata::hostInfo);
    }
}
