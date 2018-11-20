package io.simplesource.kafka.internal.streams.topology;

import io.simplesource.api.Aggregator;
import io.simplesource.api.InitialValue;
import io.simplesource.kafka.api.AggregateResources;
import io.simplesource.kafka.api.AggregateSerdes;
import io.simplesource.kafka.api.ResourceNamingStrategy;
import io.simplesource.kafka.model.*;
import io.simplesource.kafka.spec.AggregateSpec;
import lombok.Value;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;

import java.util.UUID;

/**
 * @param <A> the aggregate aggregate_update
 * @param <E> all events generated for this aggregate
 * @param <C> all commands for this aggregate
 * @param <K> the aggregate key
 */
@Value
public final class TopologyContext<K, C, E, A> {

    final AggregateSpec<K, C, E, A> aggregateSpec;
    final long commandResponseRetentionInSeconds;
    final AggregateSerdes<K, C, E, A> serdes;
    final Aggregator<E, A> aggregator;
    final InitialValue<K, A> initialValue;

    final Consumed<K, CommandRequest<K, C>> commandRequestConsumed;
    final Consumed<K, CommandResponse> commandResponseConsumed;
    final Produced<K, ValueWithSequence<E>> eventsConsumedProduced;
    final Produced<K, AggregateUpdate<A>> aggregatedUpdateProduced;
    final Produced<K, CommandResponse> commandResponseProduced;
    final Serialized<UUID, CommandResponse> serializedCommandResponse;

    public TopologyContext(AggregateSpec<K, C, E, A> aggregateSpec) {
        this.aggregateSpec = aggregateSpec;
        this.commandResponseRetentionInSeconds = aggregateSpec.generation().stateStoreSpec().retentionInSeconds();
        serdes = aggregateSpec.serialization().serdes();

        commandRequestConsumed = Consumed.with(serdes().aggregateKey(), serdes().commandRequest());
        commandResponseConsumed = Consumed.with(serdes().aggregateKey(), serdes().commandResponse());
        eventsConsumedProduced = Produced.with(serdes().aggregateKey(), serdes().valueWithSequence());
        aggregatedUpdateProduced = Produced.with(serdes().aggregateKey(), serdes().aggregateUpdate());
        commandResponseProduced = Produced.with(serdes().aggregateKey(), serdes().commandResponse());
        serializedCommandResponse = Serialized.with(serdes().commandResponseKey(), serdes().commandResponse());
        aggregator = aggregateSpec.generation().aggregator();
        initialValue = aggregateSpec.generation().initialValue();
    }

    public AggregateSerdes<K, C, E, A> serdes() {
        return aggregateSpec.serialization().serdes();
    }

    public String topicName(AggregateResources.TopicEntity entity) {
        return resourceNamingStrategy().topicName(aggregateSpec.aggregateName(), entity.name());
    }

    public String aggregateName() {
        return aggregateSpec.aggregateName();
    }

    private ResourceNamingStrategy resourceNamingStrategy() {
        return aggregateSpec.serialization().resourceNamingStrategy();
    }
}
