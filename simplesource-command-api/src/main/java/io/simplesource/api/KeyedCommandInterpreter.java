package io.simplesource.api;

import io.simplesource.data.Result;
import lombok.AllArgsConstructor;
import lombok.Value;

@Value
@AllArgsConstructor(staticName = "of")
public final class KeyedCommandInterpreter<K, E, A> {
    private final K aggregateKey;
    private final CommandInterpreter<E, A> interpreter;

    public static <K, E, A> Result<CommandError, KeyedCommandInterpreter<K, E, A>> apply(
            final K aggregateKey,
            final CommandInterpreter<E, A> interpreter) {
        return Result.success(KeyedCommandInterpreter.of(aggregateKey, interpreter));
    }
}
