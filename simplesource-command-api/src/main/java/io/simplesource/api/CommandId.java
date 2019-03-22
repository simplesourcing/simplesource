package io.simplesource.api;

import lombok.Value;

import java.util.UUID;

@Value(staticConstructor = "of")
public final class CommandId {
    public final UUID id;

    public static CommandId random() {
        return new CommandId(UUID.randomUUID());
    }
}
