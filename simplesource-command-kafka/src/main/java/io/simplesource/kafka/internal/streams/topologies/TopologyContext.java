package io.simplesource.kafka.internal.streams.topologies;

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

@Value
public class TopologyContext<K, C, E, A> {
    private final AggregateSpec<K, C, E, A> aggregateSpec;

    private Consumed<K, CommandRequest<C>> commandEventsConsumed;
    private Produced<K, ValueWithSequence<E>> eventsConsumedProduced;
    private Produced<K, AggregateUpdate<A>> aggregatedUpdateProduced;
    private Produced<K, CommandResponse> commandResponseProduced;
    private Serialized<UUID, AggregateUpdateResult<A>> serializedAggregateUpdate;

    public TopologyContext(AggregateSpec<K, C, E, A> aggregateSpec) {
        this.aggregateSpec = aggregateSpec;

        commandEventsConsumed = Consumed.with(serdes().aggregateKey(), serdes().commandRequest());
        eventsConsumedProduced = Produced.with(serdes().aggregateKey(), serdes().valueWithSequence());
        aggregatedUpdateProduced = Produced.with(serdes().aggregateKey(), serdes().aggregateUpdate());
        commandResponseProduced = Produced.with(serdes().aggregateKey(), serdes().commandResponse());
        serializedAggregateUpdate = Serialized.with(serdes().commandResponseKey(), serdes().updateResult());
    }


    public AggregateSerdes<K, C, E, A> serdes() {
        return aggregateSpec.serialization().serdes();
    }

    public String topicName(AggregateResources.TopicEntity entity) {
        return resourceNamingStrategy().topicName(aggregateSpec.aggregateName(), entity.name());
    }

    public String stateStoreName(AggregateResources.StateStoreEntity entity) {
        return resourceNamingStrategy().storeName(aggregateName(), entity.name());
    }

    public String aggregateName() {
        return aggregateSpec.aggregateName();
    }

    private ResourceNamingStrategy resourceNamingStrategy() {
        return aggregateSpec.serialization().resourceNamingStrategy();
    }
}
