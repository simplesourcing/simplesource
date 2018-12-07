package io.simplesource.kafka.api;

import io.simplesource.kafka.model.*;
import org.apache.kafka.common.serialization.Serde;

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
 * @param <C> base type of all commands for this aggregate
 * @param <E> base type of all events generated for this aggregate
 * @param <A> the aggregate aggregate_update
 */
public interface AggregateSerdes<K, C, E, A> extends CommandSerdes<K, C> {
    Serde<ValueWithSequence<E>> valueWithSequence();
    Serde<AggregateUpdate<A>> aggregateUpdate();
}
