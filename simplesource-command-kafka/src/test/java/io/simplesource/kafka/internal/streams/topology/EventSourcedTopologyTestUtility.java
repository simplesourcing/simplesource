package io.simplesource.kafka.internal.streams.topology;

import io.simplesource.api.CommandError;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.data.Sequence;
import io.simplesource.kafka.internal.streams.model.TestAggregate;
import io.simplesource.kafka.internal.streams.model.TestEvent;
import io.simplesource.kafka.model.AggregateUpdate;
import io.simplesource.kafka.model.AggregateUpdateResult;
import io.simplesource.kafka.model.ValueWithSequence;

import java.util.Optional;
import java.util.UUID;

final class EventSourcedTopologyTestUtility {

    static CommandEvents<TestEvent, Optional<TestAggregate>> buildSuccessCommandEvents(Sequence readSequence, Sequence firstEventSequence,
                                                                                       NonEmptyList<TestEvent> events) {
        return buildSuccessCommandEvents(UUID.randomUUID(), readSequence, firstEventSequence, Optional.empty(), events);
    }

    static CommandEvents<TestEvent, Optional<TestAggregate>> buildSuccessCommandEvents(Sequence readSequence,
                                                                                       NonEmptyList<TestEvent> events) {
        return buildSuccessCommandEvents(UUID.randomUUID(), readSequence, Optional.empty(), events);
    }

    static CommandEvents<TestEvent, Optional<TestAggregate>> buildSuccessCommandEvents(UUID commandId, Sequence readSequence,
                                                                                       NonEmptyList<TestEvent> events) {
        return buildSuccessCommandEvents(commandId, readSequence, Optional.empty(), events);
    }

    static CommandEvents<TestEvent, Optional<TestAggregate>> buildSuccessCommandEvents(UUID commandId, Sequence readSequence,
                                                                                       Sequence firstEventSequence,
                                                                                       Optional<TestAggregate> aggregate, NonEmptyList<TestEvent> events) {
        Sequence[] sequences = new Sequence[]{firstEventSequence};
        NonEmptyList<ValueWithSequence<TestEvent>> eventsWithSequences = events.map(e -> {
            ValueWithSequence<TestEvent> valueWithSequence = new ValueWithSequence<>(e, sequences[0]);
            sequences[0] = sequences[0].next();
            return valueWithSequence;
        });

        return new CommandEvents<>(commandId,
                readSequence, aggregate, Result.success(eventsWithSequences));
    }

    static CommandEvents<TestEvent, Optional<TestAggregate>> buildSuccessCommandEvents(UUID commandId, Sequence readSequence,
                                                                                       Optional<TestAggregate> aggregate, NonEmptyList<TestEvent> events) {
        Sequence[] sequences = new Sequence[]{readSequence};
        NonEmptyList<ValueWithSequence<TestEvent>> eventsWithSequences = events.map(e -> {
            sequences[0] = sequences[0].next();
            return new ValueWithSequence<>(e, sequences[0]);
        });

        return new CommandEvents<>(commandId,
                readSequence, aggregate, Result.success(eventsWithSequences));
    }

    static CommandEvents<TestEvent, Optional<TestAggregate>> buildFailureCommandEvents(Sequence readSequence, NonEmptyList<CommandError.Reason> errorReasons) {
        return buildFailureCommandEvents(UUID.randomUUID(), readSequence, errorReasons);
    }

    static CommandEvents<TestEvent, Optional<TestAggregate>> buildFailureCommandEvents(UUID commandId, Sequence readSequence,
                                                                                       NonEmptyList<CommandError.Reason> errorReasons) {
        return buildFailureCommandEvents(commandId, readSequence, Optional.empty(), errorReasons.map(r -> CommandError.of(r, "Error message")));
    }

    static CommandEvents<TestEvent, Optional<TestAggregate>> buildFailureCommandEvents(UUID commandId, Sequence readSequence,
                                                                                       Optional<TestAggregate> aggregate,
                                                                                       NonEmptyList<CommandError> errors) {
        return new CommandEvents<>(commandId, readSequence, aggregate, Result.failure(errors));
    }

    static AggregateUpdateResult<Optional<TestAggregate>> aggregateUpdateResultWithSuccess(Sequence readSequence,
                                                                                           AggregateUpdate<Optional<TestAggregate>> aggregateUpdate) {
        return new AggregateUpdateResult<>(UUID.randomUUID(), readSequence, Result.success(aggregateUpdate));
    }
    static AggregateUpdateResult<Optional<TestAggregate>> aggregateUpdateResultWithFailure(Sequence readSequence,
                                                                                           NonEmptyList<CommandError.Reason> errorReasons) {
        return new AggregateUpdateResult<>(UUID.randomUUID(), readSequence, Result.failure(errorReasons.map(r -> CommandError.of(r, "Error message"))));
    }

}
