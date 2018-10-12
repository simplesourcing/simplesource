
package io.simplesource.api;

import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;

/**
 * @param <E> all events generated for this aggregate
 * @param <A> the aggregate
 */
@FunctionalInterface
public interface CommandInterpretFunction<E, A> {
    /**
     * Determine if you will accept or reject the given command given the current aggregate aggregate and
     * transform into events.
     *
     * @param currentAggregate the aggregate for this aggregate guaranteed to be up to date with all events seen to date
     * @return If rejecting, return a failed <code>Result</code> providing one or more <code>Reasons</code> for the
     * rejection. If accepting return one or more events that represent the command.
     */
    Result<CommandError, NonEmptyList<E>> interpretCommand(A currentAggregate);
}
