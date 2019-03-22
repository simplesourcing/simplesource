package io.simplesource.kafka.model;

import io.simplesource.api.CommandId;
import io.simplesource.data.Sequence;
import lombok.Value;

import java.util.function.Function;

/**
 * @param <K> the aggregate key
 * @param <C> all commands for this aggregate
 */
@Value
public final class CommandRequest<K, C> {
    private CommandId commandId;
    private final K aggregateKey;
    private Sequence readSequence;
    private final C command;

    public <KR, CR> CommandRequest<KR, CR> map2(final Function<K, KR> fk, final Function<C, CR> fc) {
        return new CommandRequest<>(commandId, fk.apply(aggregateKey), readSequence, fc.apply(command));
    }
}
