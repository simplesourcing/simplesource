package io.simplesource.kafka.model;

import io.simplesource.api.CommandAPI.CommandError;
import io.simplesource.data.Sequence;
import io.simplesource.data.Result;
import lombok.AllArgsConstructor;
import lombok.Value;

import java.util.UUID;
import java.util.function.Function;

@Value
@AllArgsConstructor
public final class AggregateUpdateResult<P> {
    private UUID commandId;
    private Sequence readSequence;
    private Result<CommandError, AggregateUpdate<P>> updatedAggregateResult;

    public <S> AggregateUpdateResult<S> map(final Function<P, S> f) {
        return new AggregateUpdateResult<>(
            commandId,
            readSequence,
            updatedAggregateResult.map( pu -> pu.map(f)));
    }

}

