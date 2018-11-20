package io.simplesource.kafka.api;

public interface ResourceNamingStrategy {

    /**
     * Generate the Kafka topic name to use for a given aggregate domain.
     *
     * @param aggregateName name of the domain aggregate
     * @param topicEntity entities stored in this topic
     * @return name of the Kafka topic to use
     */
    String topicName(String aggregateName, String topicEntity);
}
