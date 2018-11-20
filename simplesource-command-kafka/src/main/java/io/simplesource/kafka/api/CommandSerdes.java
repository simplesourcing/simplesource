package io.simplesource.kafka.api;

import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.kafka.model.CommandResponse;
import org.apache.kafka.common.serialization.Serde;

import java.util.UUID;

/**
 * Responsible for providing the mechanism for reading and writing to Kafka from the Simple Sourcing
 * Kafka Streams application when using the given serialization class.
 *
 * This interface provides support for the serialization requirements of the CommandAPI.
 *
 * @param <K> the key type for aggregates, commands and events
 * @param <C> base type of all commands for this aggregate
 */
public interface CommandSerdes<K, C> {
    Serde<K> aggregateKey();
    Serde<CommandRequest<K, C>> commandRequest();
    Serde<UUID> commandResponseKey();
    Serde<CommandResponse> commandResponse();
}
