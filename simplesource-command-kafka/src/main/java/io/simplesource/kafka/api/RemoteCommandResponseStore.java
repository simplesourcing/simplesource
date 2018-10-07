package io.simplesource.kafka.api;

import io.simplesource.data.CommandError;
import io.simplesource.data.Sequence;
import io.simplesource.data.FutureResult;
import io.simplesource.data.NonEmptyList;
import org.apache.kafka.streams.state.HostInfo;

import java.time.Duration;
import java.util.UUID;

/**
 * Command responses are stored in a windowed state store.
 * When you run more than one instance of your application, each instance is given
 * a subset of partitions to manage for each state store.
 * When a user queries the state store, it's possible the partition
 * for your given aggregate could be managed remotely. In this case, KafkaStreams is
 * able to give you the <code>HostInfo</code> for the application instance that manages
 * the given aggregate. This interface is responsible for contacting the given remote
 * application instance and retrieving the aggregate values.
 */
@FunctionalInterface
public interface RemoteCommandResponseStore {

    /**
     * Get the state of the aggregate directly after the command based on the provided UUID was executed
     * from the given remote host.  If the command was successful, return the Snapshot at that time,
     * otherwise return the failure reasons.
     *
     * @param hostInfo Connection details for the remote application instance that manages the aggregate.
     * @param aggregateName the name of the aggregate that the command was executed against.
     * @param commandId the UUID of the command to lookup the result for.
     * @param timeout how long to wait attempting to fetch the result before timing out.
     * @return the result of executing the given command.
     */
    FutureResult<CommandError, NonEmptyList<Sequence>> get(HostInfo hostInfo, String aggregateName, UUID commandId, Duration timeout);

}
