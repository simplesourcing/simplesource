package io.simplesource.kafka.internal.streams.topology;

import io.simplesource.api.CommandError;
import io.simplesource.data.Sequence;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.kafka.model.ValueWithSequence;
import lombok.AllArgsConstructor;
import lombok.Value;

import java.util.UUID;

/**
 * @param <E> all events generated for this aggregate
 * @param <A> the aggregate aggregate_update
 */
@Value
@AllArgsConstructor
final class CommandEvents<E, A> {
    private final UUID commandId;
    private final Sequence readSequence;
    private final A aggregate;
    private final Result<CommandError, NonEmptyList<ValueWithSequence<E>>> eventValue;
}
