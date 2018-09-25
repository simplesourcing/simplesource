package io.simplesource.kafka.model;

import io.simplesource.data.Sequence;
import lombok.Value;

import java.util.UUID;
import java.util.function.Function;

/**
 * @param <C> all commands for this aggregate
 */
@Value
public final class CommandRequest<C> {
    private final C command;
    private final Sequence readSequence;
    private final UUID commandId;

    public <S> CommandRequest<S> map(final Function<C, S> f) {
        return new CommandRequest<>(f.apply(command), readSequence, commandId);
    }
}
