package io.simplesource.kafka.model;

import io.simplesource.api.CommandError;
import io.simplesource.data.Result;
import io.simplesource.data.Sequence;
import lombok.AllArgsConstructor;
import lombok.Value;

import java.util.UUID;

@Value
@AllArgsConstructor
public final class CommandResponse {
    private UUID commandId;
    private Sequence readSequence;
    private Result<CommandError, Sequence> sequenceResult;
}
