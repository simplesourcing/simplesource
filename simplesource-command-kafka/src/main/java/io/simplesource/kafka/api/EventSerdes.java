package io.simplesource.kafka.api;

/**
 * Responsible for providing the mechanism for reading and writing to Kafka from the Simple Sourcing
 * Kafka Streams application when using the given serialization class.
 *
 * A single instance of this class should support all aggregators that wish to persist using the given
 * serialization type. See separate serialization modules for bundled Avro and JSON implementations.
 * Most Simple Sourcing users should be able to use an out-of-the-box implementation rather than
 * writing their own.
 *
 * @param <K> the key type for aggregates, commands and events
 * @param <E> base type of all events generated for this aggregate
 */
public interface EventSerdes<K, E> extends AggregateSerdes<K, E, E, Boolean> {

}
