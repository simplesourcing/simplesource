package io.simplesource.kafka.internal.streams.statestore;

import io.simplesource.kafka.model.AggregateUpdate;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.Optional;

/**
 * Provides access to a aggregate_update state store, as required for the QueryAPI.
 * Implemented as a bridge so that we can use either a real <code>KakfaStreams</code> application
 * or a <code>TopologyTestDriver</code> based implementation.
 */
public interface AggregateStoreBridge<K, A> {
    ReadOnlyKeyValueStore<K, AggregateUpdate<A>> getAggregateStateStore();
    Optional<HostInfo> hostInfoForAggregateStoreKey(final K key);
}
