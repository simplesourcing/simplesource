package io.simplesource.kafka.internal.streams.statestore;

import io.simplesource.kafka.model.AggregateUpdateResult;
import io.simplesource.kafka.model.CommandResponse;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;

import java.util.Optional;
import java.util.UUID;

/**
 * Provides access to the command response state store, as required for the CommandAPI.
 * Implemented as a bridge so that we can use either a real <code>KakfaStreams</code> application
 * or a <code>TopologyTestDriver</code> based implementation.
 */
public interface CommandResponseStoreBridge {
    ReadOnlyWindowStore<UUID, CommandResponse> getCommandResponseStore();
    Optional<HostInfo> hostInfoForCommandResponseStoreKey(final UUID key);
}
