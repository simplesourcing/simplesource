package io.simplesource.api;

import io.simplesource.data.FutureResult;
import io.simplesource.data.NonEmptyList;
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
 * @param <K> the aggregate key
 * @param <C> all commands for this aggregate
 */
public interface CommandAPI<K, C> {

    /**
     * Submit the given command ready for processing. A successful result implies the command has been
     * successfully queued ready to be processed by the command handler. The validation of the command
     * and translation into events is done asynchronously after publishing a command and is not reflected
     * in the result of this method.
     *
     * @param request command request.
     * @param timeout how long to wait for processing to complete and the result to be available.
     * @return a <code>FutureResult</code> with the sequence number of aggregate.
     */
    FutureResult<CommandError, Sequence> publishCommand(
            final Request<K, C> request,
            final Duration timeout
    );

    /**
     * Get the result of the execution of the command identified by the provided UUID.
     * If the command was successful, return sequence numbers of all generated events.
     * If the command fails, return the failure reasons. Implementations of this method
     * are permitted to have limited retention when querying commands.
     *
     * If a command is queried outside the retention window it will keep trying for the
     * given timeout duration then fail with a <code>Timeout</code> error code.
     *
     * This method can be called to validate the result of the <code>publishCommand</code> method
     * In cases where there may have been a fault such as a network failure, and the future
     * returned by <code>publishCommand</code> did not complete in time.
     *
     * @param commandId the UUID of the command to lookup the result for.
     * @param timeout how long to wait attempting to fetch the result before timing out.
     * @return sequence number of aggregate.
     */
    FutureResult<CommandError, Sequence> queryCommandResult(
        UUID commandId,
        Duration timeout
    );

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
