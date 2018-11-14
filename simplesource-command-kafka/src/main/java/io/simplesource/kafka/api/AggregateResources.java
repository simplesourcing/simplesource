package io.simplesource.kafka.api;

public final class AggregateResources {

    public enum TopicEntity {
        command_request,
        event,
        aggregate,
        command_response,
        command_response_topic_map,
    }

    public enum StateStoreEntity {
        command_response,
        aggregate_update,
        projection_update
    }
}
