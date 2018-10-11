package io.simplesource.api;

import io.simplesource.data.Result;
import lombok.AllArgsConstructor;
import lombok.Value;

@Value
@AllArgsConstructor(staticName = "apply")
public final class KeyedCommandInterpreter<K, E, A> {
    private final Result<CommandError, K> aggregateKey;
    private final CommandInterpreter<E, A> interpreter;
}
