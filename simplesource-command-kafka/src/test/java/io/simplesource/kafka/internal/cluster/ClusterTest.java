package io.simplesource.kafka.internal.cluster;

import io.simplesource.api.CommandAPI;
import io.simplesource.data.*;
import io.simplesource.kafka.internal.util.NamedThreadFactory;
import org.apache.kafka.streams.state.HostInfo;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

//TODO negative test paths
public final class ClusterTest {

    private static final String SUCCESS_AGGREGATE = "SuccessAggregate";
    private static final Result<CommandError, NonEmptyList<Sequence>> SUCCESS_RESULT =
            Result.success(NonEmptyList.of(Sequence.position(new Random().nextLong())));

    private final CommandAPI<Object, Object> succsssCommandPI = new CommandAPI<Object, Object>() {
        @Override
        public FutureResult<CommandError, UUID> publishCommand(Request<Object, Object> request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FutureResult<CommandError, NonEmptyList<Sequence>> queryCommandResult(UUID commandId, Duration timeout) {
            return FutureResult.ofSupplier(() -> SUCCESS_RESULT);
        }
    };

    private final Map<String, CommandAPI<?, ?>> aggregates = new HashMap<String, CommandAPI<?, ?>>() {{
        put(SUCCESS_AGGREGATE, succsssCommandPI);
    }};

    //TODO move to util class
    static <T> CompletableFuture<List<T>> sequence(List<CompletableFuture<T>> completableFutures) {
        return CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture<?>[completableFutures.size()]))
                .thenApply(v -> completableFutures.stream().map(CompletableFuture::join).collect(Collectors.toList()));
    }

    @Test
    public void testSuccessRun() throws Exception {
         int loopCount = 1000;
         int server1Port = randomPort();
         int server2Port = randomPort();

        ClusterSubsystem subsystem1 = new ClusterSubsystem((a) ->aggregates.get(a), new ClusterConfig().port(server1Port),
                Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("Cluster host 1 scheduler")));

        ClusterSubsystem subsystem2 = new ClusterSubsystem((a) -> aggregates.get(a), new ClusterConfig().port(server2Port),
                Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("Cluster host 2 scheduler")));

        subsystem1.start();
        subsystem2.start();

        List<CompletableFuture<Result<CommandError, NonEmptyList<Sequence>>>> futures = new ArrayList<>();

        long start = System.currentTimeMillis();
        for (int i = 0; i < loopCount; i++) {
            futures.add(subsystem1.get(new HostInfo("127.0.0.1", server2Port), SUCCESS_AGGREGATE, UUID.randomUUID(),
                    Duration.ofSeconds(120)).future());
        }

        List<Result<CommandError, NonEmptyList<Sequence>>> results = sequence(futures).get();

        System.out.print("Processed: " + loopCount + " requests in " + ((System.currentTimeMillis()-start) / 1000) + " seconds");

        for (int i = 0; i < loopCount; i++) {
            assertEquals(results.get(i), SUCCESS_RESULT);
        }


        subsystem2.stop();
        subsystem1.stop();
    }


    private static int randomPort () {
        int min = 1050;
        int max = 15000;
        return min + (int)(Math.random() * ((max - min) + 1));
    }
}
