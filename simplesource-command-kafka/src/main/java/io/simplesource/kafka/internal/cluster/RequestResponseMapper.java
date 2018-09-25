package io.simplesource.kafka.internal.cluster;

import io.simplesource.api.CommandAPI;
import io.simplesource.data.Sequence;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import org.apache.kafka.streams.state.HostInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

class RequestResponseMapper {

    private static final Logger logger = LoggerFactory.getLogger(RequestResponseMapper.class);

    private static final Result<CommandAPI.CommandError, NonEmptyList<Sequence>> REMOTE_LOOKUP_TIMED_OUT =
            Result.failure(CommandAPI.CommandError.Timeout, "Remote lookup timed out");

    private final ScheduledExecutorService scheduler;
    private final AtomicLong idGenerator = new AtomicLong(0);
    private final Map<Long, CompletableFuture<Result<CommandAPI.CommandError, NonEmptyList<Sequence>>>> requestMap = new ConcurrentHashMap<>();
    private final HostInfo sourceHost;

    RequestResponseMapper(HostInfo sourceHost, ScheduledExecutorService scheduler) {
        this.sourceHost = sourceHost;
        this.scheduler = scheduler;
    }

    //TODO we should build in circuit break here to make sure we do not overload subsystem.
    MappedCommandRequest newCommandRequest(HostInfo targetHost, String aggregateName, UUID commandUUId, Duration timeout) {
        Long requestId = idGenerator.incrementAndGet();
        final CompletableFuture<Result<CommandAPI.CommandError, NonEmptyList<Sequence>>> cf = new CompletableFuture<>();

        long delayInSeconds = timeout.getSeconds();

        if (delayInSeconds < 1) {
            cf.complete(REMOTE_LOOKUP_TIMED_OUT);
        } else {
            requestMap.put(requestId, cf);
            scheduler.schedule(() -> completeRequest(Message.response(requestId, REMOTE_LOOKUP_TIMED_OUT),true),
                    Math.round(timeout.getSeconds() * 1.25), TimeUnit.SECONDS);
        }

        return new MappedCommandRequest(requestId, targetHost, Message.request(requestId, sourceHost, aggregateName, commandUUId,timeout),cf);
    }

    private void completeRequest(Message.CommandResponse commandResponse, boolean scheduled) {
        CompletableFuture<Result<CommandAPI.CommandError, NonEmptyList<Sequence>>> future = requestMap.remove(commandResponse.requestId);
        if (future != null) {
            future.complete(commandResponse.result);
        } else {
            if (!scheduled) {
                logger.warn("No request mapping found for response:{}", commandResponse);
            }
        }
    }

    void completeRequest(Message.CommandResponse commandResponse) {
       completeRequest(commandResponse,false);
    }

}
