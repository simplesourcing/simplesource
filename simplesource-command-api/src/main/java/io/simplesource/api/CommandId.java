package io.simplesource.api;

import lombok.Value;

import java.util.UUID;

@Value(staticConstructor = "of")
public final class CommandId implements UuidId {
    private final UUID id;
}
