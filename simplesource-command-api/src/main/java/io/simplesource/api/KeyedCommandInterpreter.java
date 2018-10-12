package io.simplesource.api;

import lombok.AllArgsConstructor;
import lombok.Value;

@Value
@AllArgsConstructor(staticName = "apply")
public final class KeyedCommandInterpreter<K, E, A> {
    private final K aggregateKey;
    private final CommandInterpreter<E, A> interpreter;
}
