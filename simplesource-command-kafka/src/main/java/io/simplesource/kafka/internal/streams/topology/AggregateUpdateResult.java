package io.simplesource.kafka.internal.streams.topology;

import io.simplesource.api.CommandError;
import io.simplesource.data.Sequence;
import io.simplesource.data.Result;
import io.simplesource.kafka.model.AggregateUpdate;
import lombok.AllArgsConstructor;
import lombok.Value;

import java.util.UUID;

@Value
@AllArgsConstructor
final class AggregateUpdateResult<A> {
    private UUID commandId;
    private Sequence readSequence;
    private Result<CommandError, AggregateUpdate<A>> updatedAggregateResult;
}
