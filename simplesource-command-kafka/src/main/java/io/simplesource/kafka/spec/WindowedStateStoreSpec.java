package io.simplesource.kafka.spec;

import lombok.Value;

@Value
public final class WindowedStateStoreSpec {
    private final long retentionInSeconds;
}
