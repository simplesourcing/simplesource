package io.simplesource.kafka.internal.streams;

import io.simplesource.api.CommandError;
import io.simplesource.api.CommandInterpreter;
import io.simplesource.data.Sequence;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.kafka.model.AggregateUpdateResult;
import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.kafka.model.ValueWithSequence;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.kafka.streams.kstream.KStream;

import java.util.UUID;

@Value
@AllArgsConstructor
final class CommandEvents<E, A> {
    private final UUID commandId;
    private final Sequence readSequence;
    private final A aggregate;
    private final Result<CommandError, NonEmptyList<ValueWithSequence<E>>> eventValue;
}


@Value
@AllArgsConstructor(staticName = "apply")
final class KeyedCommandInterpreter2<K, C, E, A> {
    private final K aggregateKey;
    private final CommandRequest<C> request;
    private final CommandInterpreter<E, A> interpreter;
}

@Value
@AllArgsConstructor(staticName = "apply")
final class KeyedCommandInput<K, C, E, A> {
    private final CommandRequest<C> request;
    private final UUID commandId;
    private final Result<CommandError, KeyedCommandInterpreter2<K, C, E, A>> interpreter;
}

@Value
@AllArgsConstructor
final class CommandInterpreterStreams<K, C, E, A> {
    private final KStream<K, KeyedCommandInterpreter2<K, C, E, A>> successStream;
    private final KStream<UUID, AggregateUpdateResult<A>> failureStream;
}
