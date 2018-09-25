package io.simplesource.kafka.api;

public final class AggregateResources {

    public enum TopicEntity {
        command_request,
        event,
        aggregate
    }

    public enum StateStoreEntity {
        command_response,
        aggregate_update,
        projection_update
    }
}
