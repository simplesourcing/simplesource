package io.simplesource.kafka.internal.streams.topologies;

import io.simplesource.api.Aggregator;
import io.simplesource.api.CommandError;
import io.simplesource.data.Result;
import io.simplesource.kafka.model.AggregateUpdate;
import io.simplesource.kafka.model.AggregateUpdateResult;
import io.simplesource.kafka.model.ValueWithSequence;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Collections;
import java.util.function.BiFunction;

import static io.simplesource.kafka.api.AggregateResources.TopicEntity.aggregate;

final class EventProcessingSubTopology<K, E, A> {

    private final AggregateStreamResourceNames aggregateStreamResourceNames;
    private final Aggregator<E, A> aggregator;
    private final Produced<K, AggregateUpdate<A>> aggregatedUpdateProduced;

    EventProcessingSubTopology(AggregateStreamResourceNames aggregateStreamResourceNames, Aggregator<E, A> aggregator,
                               Serde<K> keySerde, Serde<AggregateUpdate<A>> aggregateUpdateSerde) {
        this.aggregateStreamResourceNames = aggregateStreamResourceNames;
        this.aggregator = aggregator;
        this.aggregatedUpdateProduced = Produced.with(keySerde, aggregateUpdateSerde);
    }

    KStream<K, AggregateUpdateResult<A>> add(KStream<K, CommandEvents<E, A>> eventResultStream) {
        KStream<K, AggregateUpdateResult<A>> aggregateUpdateStream = aggregateUpdateStream(eventResultStream);
        publishAggregateUpdates(aggregateUpdateStream);
        return aggregateUpdateStream;
    }

    private KStream<K, AggregateUpdateResult<A>> aggregateUpdateStream(final KStream<K, CommandEvents<E, A>> eventResultStream) {
        return eventResultStream
                .mapValues((serializedKey, result) -> {
                    final Result<CommandError, AggregateUpdate<A>> aggregateUpdateResult = result.eventValue().map(events -> {
                        final BiFunction<AggregateUpdate<A>, ValueWithSequence<E>, AggregateUpdate<A>> reducer =
                                (aggregateUpdate, eventWithSequence) -> new AggregateUpdate<>(
                                        aggregator.applyEvent(aggregateUpdate.aggregate(), eventWithSequence.value()),
                                        eventWithSequence.sequence()
                                );
                        return events.fold(
                                eventWithSequence -> new AggregateUpdate<>(
                                        aggregator.applyEvent(result.aggregate(), eventWithSequence.value()),
                                        eventWithSequence.sequence()
                                ),
                                reducer
                        );
                    });
                    return new AggregateUpdateResult<>(
                            result.commandId(),
                            result.readSequence(),
                            aggregateUpdateResult);
                });
    }

    private void publishAggregateUpdates(final KStream<K, AggregateUpdateResult<A>> aggregateUpdateStream) {
        final KStream<K, AggregateUpdate<A>> aggregateStream = aggregateUpdateStream
                .flatMapValues(update -> update.updatedAggregateResult().fold(
                        reasons -> Collections.emptyList(),
                        Collections::singletonList
                ));
        aggregateStream.to(aggregateStreamResourceNames.topicName(aggregate), aggregatedUpdateProduced);
    }
}
