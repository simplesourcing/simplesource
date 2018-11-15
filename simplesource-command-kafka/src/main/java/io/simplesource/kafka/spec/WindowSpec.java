package io.simplesource.kafka.spec;

import lombok.Value;

@Value
public final class WindowSpec {
    private final long retentionInSeconds;
}
