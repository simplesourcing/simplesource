package io.simplesource.kafka.api;

public final class AggregateResources {

    public enum TopicEntity {
        command_request,
        event,
        aggregate,
        command_response,
        command_response_topic_map,
    }
}
