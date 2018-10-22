package io.simplesource.kafka.internal.streams.topologies;

import io.simplesource.kafka.model.AggregateUpdate;
import io.simplesource.kafka.model.AggregateUpdateResult;
import io.simplesource.kafka.model.CommandResponse;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import static io.simplesource.kafka.api.AggregateResources.TopicEntity.command_response;

final class CommandResponsePublishConsumer<K, A> implements AggregateUpdateResultStreamConsumer<K, A> {
    private final AggregateStreamResourceNames aggregateStreamResourceNames;
    private final Produced<K, CommandResponse> commandResponseProduced;

    CommandResponsePublishConsumer(AggregateStreamResourceNames aggregateStreamResourceNames, Serde<K> keySerde,
                                          Serde<CommandResponse> commandResponseSerde) {
        this.aggregateStreamResourceNames = aggregateStreamResourceNames;
        commandResponseProduced = Produced.with(keySerde, commandResponseSerde);
    }

    @Override
    public void accept(KStream<K, AggregateUpdateResult<A>> stream) {
        final KStream<K, CommandResponse> aggregateStream = stream
                .mapValues((key, update) ->
                        new CommandResponse(update.commandId(), update.readSequence(),
                                update.updatedAggregateResult().map(AggregateUpdate::sequence))
                );
        aggregateStream.to(aggregateStreamResourceNames.topicName(command_response), commandResponseProduced);
    }
}
