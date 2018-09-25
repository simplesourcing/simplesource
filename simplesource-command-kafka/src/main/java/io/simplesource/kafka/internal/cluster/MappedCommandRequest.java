package io.simplesource.kafka.internal.cluster;

import io.simplesource.api.CommandAPI;
import io.simplesource.data.Sequence;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.kafka.streams.state.HostInfo;

import java.util.concurrent.CompletableFuture;

@Value
@AllArgsConstructor
class MappedCommandRequest {

    private long requestId;
    private HostInfo toHostInfo;
    private Message.CommandRequest commandRequest;
    private CompletableFuture<Result<CommandAPI.CommandError, NonEmptyList<Sequence>>> completableFuture;

}
