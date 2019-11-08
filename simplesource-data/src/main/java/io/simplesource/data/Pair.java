package io.simplesource.data;

import lombok.Value;

/**
 * https://stackoverflow.com/a/28526052/2431728
 */
@Value(staticConstructor = "of")
public class Pair<A, B> {
    private final A _1;
    private final B _2;
}
