package io.simplesource.kafka.internal.streams;

import io.simplesource.api.InvalidSequenceHandler;
import io.simplesource.api.CommandError;
import io.simplesource.api.CommandError.Reason;
import io.simplesource.kafka.dsl.InvalidSequenceStrategy;

import java.util.Optional;

public final class InvalidSequenceHandlerProvider {
    public static <K, C, A> InvalidSequenceHandler<K, C, A> getForStrategy(InvalidSequenceStrategy strategy) {

        if (strategy == InvalidSequenceStrategy.LastWriteWins)
            return (key, expectedSeq, currentSeq, currentAggregate, command) -> Optional.empty();

        if (strategy == InvalidSequenceStrategy.Strict)
            return (key, expectedSeq, currentSeq, currentAggregate, command) ->
                    Optional.of(CommandError.of(Reason.InvalidReadSequence,
                        String.format("Command received with read sequence %1$d when expecting %2$d",
                            currentSeq.getSeq(), expectedSeq.getSeq())));

        throw new IllegalArgumentException(String.format("Unrecognised InvalidSequenceStrategy %s", strategy));
    }
}
