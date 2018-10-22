package io.simplesource.kafka.internal.streams.topologies;

import io.simplesource.kafka.model.AggregateUpdateResult;
import org.apache.kafka.streams.kstream.KStream;

import java.util.function.Consumer;

/**
 * Consumer of aggregate update result stream. Basically it could be considered
 * as {@link org.apache.kafka.streams.TopologyDescription.Sink} except that it could be used to update state store.
 * @param <K> the aggregate key
 * @param <A> the aggregate
 */
@FunctionalInterface
interface AggregateUpdateResultStreamConsumer<K, A> extends Consumer<KStream<K, AggregateUpdateResult<A>>> {
}
