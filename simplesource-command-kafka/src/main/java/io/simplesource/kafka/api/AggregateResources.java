package io.simplesource.kafka.api;

public final class AggregateResources {

    public enum TopicEntity {
        COMMAND_REQUEST,
        EVENT,
        AGGREGATE,
        COMMAND_RESPONSE,
        COMMAND_RESPONSE_TOPIC_MAP,
    }
}
