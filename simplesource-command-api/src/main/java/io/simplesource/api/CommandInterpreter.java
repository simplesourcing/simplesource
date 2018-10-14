package io.simplesource.api;

import io.simplesource.data.Result;
import lombok.AllArgsConstructor;
import lombok.Value;

@Value
@AllArgsConstructor(staticName = "of")
public final class CommandInterpreter<K, E, A> {
    private final K aggregateKey;
    private final CommandInterpretFunction<E, A> interpreter;

    public static <K, E, A> Result<CommandError, CommandInterpreter<K, E, A>> apply(
            final K aggregateKey,
            final CommandInterpretFunction<E, A> interpreter) {
        return Result.success(CommandInterpreter.of(aggregateKey, interpreter));
    }
}

