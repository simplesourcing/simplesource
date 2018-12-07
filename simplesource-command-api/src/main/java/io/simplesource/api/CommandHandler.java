package io.simplesource.api;

import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;

/**
 * A command handler is responsible for accepting or rejecting commands and turning accepted commands into a list of
 * events by applying the appropriate business logic and validations based on the current state. To assist in
 * determining if a command is valid, the command handler has an internal aggregator that parses events as they are
 * generated. The internal aggregator is guaranteed to be fully up-to-date at the point a new command is interpreted
 * with the latest aggregate provided as an argument ready to use for validation and business logic.
 *
 * @param <K> the aggregate key type
 * @param <C> all commands for this aggregate
 * @param <E> all events generated for this aggregate
 * @param <A> the aggregate type
 */
@FunctionalInterface
public interface CommandHandler<K, C, E, A> {
    /**
     * Determine whether you will accept the given command given the current aggregate and, if so, transform it into events.
     *
     * @param key              the aggregate key the command applies to
     * @param currentAggregate the aggregate for this aggregate guaranteed to be up to date with all events seen to date
     * @param command          the command to validate and convert into events
     * @return If rejecting, return a failed <code>Result</code> providing one or more <code>Reasons</code> for the
     * rejection. If accepting, return one or more events that represent the command.
     */
    Result<CommandError, NonEmptyList<E>> interpretCommand(
            K key,
            A currentAggregate,
            C command);
}
