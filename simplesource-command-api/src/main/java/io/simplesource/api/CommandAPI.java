package io.simplesource.api;

import io.simplesource.data.FutureResult;
import io.simplesource.data.Sequence;
import lombok.Value;

import java.time.Duration;
import java.util.UUID;

/**
 * The public API for submitting commands against a given aggregate and
 * querying where they have been successfully applied.
 *
 * For a command to be successfully applied, this sequence must be the same as the sequence id of the last applied event.
 *
 * @param <K> the aggregate key type
 * @param <C> all commands for this aggregate
 */
public interface CommandAPI<K, C> {

    /**
     * Submit the given command ready for processing. A successful result implies the command has been
     * successfully queued ready to be processed by the command handler. The validation of the command
     * and translation into events is done asynchronously after publishing and is not reflected
     * in the result of this method.
     *
     * @param request command request.
     * @return a <code>FutureResult</code> with the commandId echoed back if the command was successfully queued,
     * otherwise a list of reasons for the failure.
     */
    FutureResult<CommandError, UUID> publishCommand(Request<K, C> request);

    /**
     * Get the result of the execution of the command identified by the provided UUID.
     * If the command was successful, return the highest sequence number of the generated events.
     * If the command fails, return the failure reasons. Implementations of this method
     * are permitted to have limited retention when querying commands.
     *
     * If a command is queried outside the retention window it will keep trying for the
     * given timeout duration then fail with a <code>Timeout</code> error code.
     *
     * @param commandId the UUID of the command to lookup the result for.
     * @param timeout how long to wait attempting to fetch the result before timing out.
     * @return sequence number of aggregate.
     */
    FutureResult<CommandError, Sequence> queryCommandResult(
        UUID commandId,
        Duration timeout
    );

    /**
     * Chain together publishing a command then query the result.
     * @param commandRequest the command request.
     * @param timeout how long to wait for processing to complete and the result to be available.
     * @return sequence number of aggregate.
     */
    default FutureResult<CommandError, Sequence> publishAndQueryCommand(
        final Request<K, C> commandRequest,
        final Duration timeout
    ) {
        return publishCommand(commandRequest)
            .flatMap(v -> queryCommandResult(commandRequest.commandId, timeout));
    }

    @Value
    class Request<K, C> {
        // the aggregate key the command applies to
        private final K key;
        // the version of the aggregate this command is based on
        private final Sequence readSequence;
        // unique id for this command
        private final UUID commandId;
        // the command we wish to apply to the aggregate
        private final C command;
    }

}
