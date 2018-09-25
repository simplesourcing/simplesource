package io.simplesource.api;

import io.simplesource.api.CommandAPI.CommandError;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.data.Sequence;

import java.util.Objects;

import static io.simplesource.data.Result.failure;

/**
 * A command handler is responsible for accepting or rejecting commands and turning accepted commands into a list of
 * events by applying the appropriate business logic and validations based on the current state. To assist in
 * determining if a command is valid, the command handler has an internal aggregator that parses events as they are
 * generated. The internal aggregator is guaranteed to be fully up-to-date at the point a new command is interpreted
 * with the latest aggregate provided as an argument ready to use for validation and business logic.
 *
 * @param <K> the aggregate key
 * @param <C> all commands for this aggregate
 * @param <E> all events generated for this aggregate
 * @param <A> the aggregate
 */
@FunctionalInterface
public interface CommandHandler<K, C, E, A> {
    /**
     * Determine if you will accept or reject the given command given the current aggregate aggregate and
     * transform into events.
     *
     * @param key              the aggregate key the command applies to
     * @param expectedSeq      the sequence we expect the current aggregate to be at
     * @param currentSeq       the actual seq number of the current aggregate
     * @param currentAggregate the aggregate for this aggregate guaranteed to be up to date with all events seen to date
     * @param command          the command to validate and convert into events
     * @return If rejecting, return a failed <code>Result</code> providing one or more <code>Reasons</code> for the
     * rejection. If accepting return one or more events that represent the command.
     */
    Result<CommandError, NonEmptyList<E>> interpretCommand(
            K key,
            Sequence expectedSeq,
            Sequence currentSeq,
            A currentAggregate,
            C command);

    /**
     * This method will wrap a {@link CommandHandler} in a check to ensure that the current aggregate sequence
     * matches the expected aggregate sequence. This is effectively an optimistic lock on the Aggregate.
     *
     * @param <K> the aggregate key
     * @param <C> all commands for this aggregate
     * @param <E> all events generated for this aggregate
     * @param <A> the aggregate
     * @param commandHandler the command handler that will be invoked if the sequence numbers match
     * @return If rejecting, return a failed <code>Result</code> providing one or more <code>Reasons</code> for the
     * rejection. If accepting return one or more events that represent the command.
     */
    static <K, C, E, A> CommandHandler<K, C, E, A> ifSeq(final CommandHandler<K, C, E, A> commandHandler) {
        return (key, expectedSeq, currentSeq, currentAggregate, command) ->
                Objects.equals(expectedSeq, currentSeq)
                        ? commandHandler.interpretCommand(key, expectedSeq, currentSeq, currentAggregate, command)
                        : failure(CommandAPI.CommandError.InvalidReadSequence,
                        String.format("Command received with read sequence %1$d when expecting %2$d",
                                currentSeq.getSeq(), expectedSeq.getSeq()));
    }
}
