package io.simplesource.kafka.model;

import io.simplesource.api.CommandError;
import io.simplesource.data.Result;
import io.simplesource.data.Sequence;
import lombok.AllArgsConstructor;
import lombok.Value;

import java.util.UUID;
import java.util.function.Function;

@Value
@AllArgsConstructor
public final class CommandResponse<K> {
    private final K aggregateKey;
    private UUID commandId;
    private Sequence readSequence;
    private Result<CommandError, Sequence> sequenceResult;

    public <KR> CommandResponse<KR> map(final Function<K, KR> fk) {
        return new CommandResponse<>(fk.apply(aggregateKey), commandId, readSequence, sequenceResult);
    }
}
