package io.simplesource.kafka.internal.streams.statestore;

import io.simplesource.data.FutureResult;
import io.simplesource.data.Result;
import io.simplesource.kafka.internal.util.RetryDelay;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.StreamsMetadata;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public final class StateStoreUtils {

    public static <E, R> FutureResult<E, R> get(
        final Supplier<Result<E, HostInfo>> hostInfoForAggregate,
        final HostInfo localHostInfo,
        final Supplier<Optional<Result<E, R>>> localReader,
        final BiFunction<HostInfo, Duration, FutureResult<E, R>> remoteReader,
        final Supplier<E> timeoutErrorSupplier,
        final Function<Exception, E> errorMapper,
        final ScheduledExecutorService scheduledExecutor,
        final RetryDelay retryDelay,
        final Duration timeout
    ) {
        return spin(
            hostInfoForAggregate,
            localHostInfo,
            localReader,
            remoteReader,
            timeoutErrorSupplier,
            errorMapper,
            scheduledExecutor,
            retryDelay,
            0L,
            System.currentTimeMillis(),
            timeout.toMillis(),
            0);
    }

    //TODO way too many args - encapsulate
    private static <E, R> FutureResult<E, R> spin(
        final Supplier<Result<E, HostInfo>> hostInfoForAggregate,
        final HostInfo localHostInfo,
        final Supplier<Optional<Result<E, R>>> localReader,
        final BiFunction<HostInfo, Duration, FutureResult<E, R>> remoteReader,
        final Supplier<E> timeoutErrorSupplier,
        final Function<Exception, E> errorMapper,
        final ScheduledExecutorService scheduledExecutor,
        final RetryDelay retryDelay,
        final long delayInMs,
        final long start,
        final long timeout,
        final int spinCount) {

        return (delayInMs <= 0L ?
            FutureResult.ofSupplier(hostInfoForAggregate) :
            FutureResult.ofFutureResult(scheduledExecutor.schedule(hostInfoForAggregate::get, delayInMs, TimeUnit.MILLISECONDS), errorMapper))
            .flatMap(aggregateHostInfo -> spinImpl(
                aggregateHostInfo,
                hostInfoForAggregate,
                localHostInfo,
                localReader,
                remoteReader,
                timeoutErrorSupplier,
                errorMapper,
                scheduledExecutor,
                retryDelay,
                start,
                timeout,
                spinCount));
    }

    //TODO introduce clock interface for testing this
    private static <E, R> FutureResult<E, R> spinImpl(
        final HostInfo aggregateHostInfo,
        final Supplier<Result<E, HostInfo>> hostInfoForAggregate,
        final HostInfo localHostInfo,
        final Supplier<Optional<Result<E, R>>> localReader,
        final BiFunction<HostInfo, Duration, FutureResult<E, R>> remoteReader,
        final Supplier<E> timeoutErrorSupplier,
        final Function<Exception, E> errorMapper,
        final ScheduledExecutorService scheduledExecutor,
        final RetryDelay retryDelay,
        final long start,
        final long timeout,
        final int spinCount) {

        if (Objects.equals(aggregateHostInfo, StreamsMetadata.NOT_AVAILABLE.hostInfo())) {
            // stream app isn't running yet
            return spin(hostInfoForAggregate,
                localHostInfo,
                localReader,
                remoteReader,
                timeoutErrorSupplier,
                errorMapper,
                scheduledExecutor,
                retryDelay,
                retryDelay.delay(start, timeout, spinCount),
                start,
                timeout,
                spinCount + 1);
        }

        if (!Objects.equals(aggregateHostInfo, localHostInfo)) {
            // reduce timeout to get around clock skew, just start with new timeout that knows about
            // previous spin time
            final long newTimeout = Math.max(0L, timeout - (System.currentTimeMillis() - start));
            return remoteReader.apply(aggregateHostInfo, Duration.ofMillis(newTimeout));
        } else {
            return FutureResult.<E, Optional<Result<E, R>>>of(localReader.get()).flatMap(loaded -> {
                if (loaded.isPresent()) {
                    return FutureResult.ofSupplier(loaded::get);
                }

                if (timeout < System.currentTimeMillis() - start) {
                    return FutureResult.fail(timeoutErrorSupplier.get(), "Request timed out");
                }
                // TODO make stack safe
                return spin(hostInfoForAggregate,
                    localHostInfo,
                    localReader,
                    remoteReader,
                    timeoutErrorSupplier,
                    errorMapper,
                    scheduledExecutor,
                    retryDelay,
                    retryDelay.delay(start, timeout, spinCount),
                    start,
                    timeout,
                    spinCount + 1);
            });
        }
    }

}
