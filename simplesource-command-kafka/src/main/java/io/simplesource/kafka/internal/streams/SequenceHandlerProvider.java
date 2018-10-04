package io.simplesource.kafka.internal.streams;

import io.simplesource.api.CommandAPI;
import io.simplesource.api.SequenceHandler;
import io.simplesource.data.Reason;
import io.simplesource.kafka.dsl.CommandSequenceStrategy;

import java.util.Objects;
import java.util.Optional;

public final class SequenceHandlerProvider {
    public static <K, C, A> SequenceHandler<K, C, A> getForStrategy(CommandSequenceStrategy strategy) {

        if (strategy == CommandSequenceStrategy.LastWriteWins)
                return (key, expectedSeq, currentSeq, currentAggregate, command) -> Optional.empty();

        if (strategy == CommandSequenceStrategy.Strict)
            return (key, expectedSeq, currentSeq, currentAggregate, command) ->
            Objects.equals(expectedSeq, currentSeq)
                    ? Optional.empty()
                    : Optional.of(Reason.of(CommandAPI.CommandError.InvalidReadSequence,
                    String.format("Command received with read sequence %1$d when expecting %2$d",
                            currentSeq.getSeq(), expectedSeq.getSeq())));

        throw new IllegalArgumentException(String.format("Unrecognised CommandSequenceStrategy %s", strategy));
    }
}
