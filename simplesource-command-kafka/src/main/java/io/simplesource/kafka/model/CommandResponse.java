package io.simplesource.kafka.model;

import io.simplesource.api.CommandError;
import io.simplesource.api.CommandId;
import io.simplesource.data.Result;
import io.simplesource.data.Sequence;
import lombok.AllArgsConstructor;
import lombok.Value;

import java.util.UUID;
import java.util.function.Function;

@Value
@AllArgsConstructor
public final class CommandResponse<K> {
    private CommandId commandId;
    private final K aggregateKey;
    private Sequence readSequence;
    private Result<CommandError, Sequence> sequenceResult;

    public <KR> CommandResponse<KR> map(final Function<K, KR> fk) {
        return new CommandResponse<>(commandId, fk.apply(aggregateKey), readSequence, sequenceResult);
    }
}
