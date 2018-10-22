package io.simplesource.kafka.internal.streams.topologies;

import io.simplesource.kafka.api.AggregateResources;
import io.simplesource.kafka.api.ResourceNamingStrategy;

final class AggregateStreamResourceNames {
    private final ResourceNamingStrategy resourceNamingStrategy;
    private final String aggregateName;

    public AggregateStreamResourceNames(ResourceNamingStrategy resourceNamingStrategy, String aggregateName) {
        this.resourceNamingStrategy = resourceNamingStrategy;
        this.aggregateName = aggregateName;
    }

    public String topicName(AggregateResources.TopicEntity entity) {
        return resourceNamingStrategy.topicName(aggregateName, entity.name());
    }

    public String stateStoreName(AggregateResources.StateStoreEntity entity) {
        return resourceNamingStrategy.storeName(aggregateName, entity.name());
    }

    public String getAggregateName() {
        return aggregateName;
    }
}