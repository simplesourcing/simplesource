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
    private final K aggregateKey;
    private final C command;
    private final Sequence readSequence;
    private final CommandId commandId;

    public <KR, CR> CommandRequest<KR, CR> map2(final Function<K, KR> fk, final Function<C, CR> fc) {
        return new CommandRequest<>(fk.apply(aggregateKey), fc.apply(command), readSequence, commandId);
    }
}
