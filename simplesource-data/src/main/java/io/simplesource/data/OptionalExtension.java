package io.simplesource.data;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

public class OptionalExtension {
    public static <A, B> Optional<Pair<A, B>> zip(Optional<A> maybeA, Optional<B> maybeB) {
        return maybeA.flatMap((A a) -> maybeB.map((B b) -> Pair.of(a, b)));
    }

    public static <A, R> R fold(Optional<A> maybeA, Supplier<R> onNone, Function<A, R> onSome) {
        return maybeA.map(onSome).orElseGet(onNone);
    }
}
