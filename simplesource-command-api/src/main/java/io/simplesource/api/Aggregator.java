package io.simplesource.api;

/**
 * An Aggregator is a function that builds up an aggregate from a stream of events (effectively a fold function).
 *
 * In Simple Sourcing, an Aggregate has one primary Aggregator that is guaranteed to be up-to-date with all events
 * prior to executing a command against the aggregate.
 *
 * @param <E> all events generated for this aggregate
 * @param <A> the aggregate type
 */
@FunctionalInterface
public interface Aggregator<E, A> {
    /**
     * A function that takes the current aggregate value, the latest event and generates a new aggregate value. If no
     * Aggregate exists, the {@link InitialValue} is used to create the starting value.
     *
     * @param currentAggregate the current aggregate value for this aggregate
     * @param event the latest event to apply to this aggregate
     * @return the updated aggregate value for this aggregate after the given event has been applied
     */
    A applyEvent(A currentAggregate, E event);
}
