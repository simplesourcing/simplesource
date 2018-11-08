package io.simplesource.kafka.internal;

import io.simplesource.api.CommandAPI;
import io.simplesource.api.CommandError;
import io.simplesource.data.*;
import io.simplesource.kafka.api.AggregateSerdes;
import io.simplesource.kafka.api.RemoteCommandResponseStore;
import io.simplesource.kafka.dsl.KafkaConfig;
import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.kafka.model.AggregateUpdateResult;
import io.simplesource.kafka.internal.streams.statestore.CommandResponseStoreBridge;
import io.simplesource.kafka.internal.streams.statestore.StateStoreUtils;
import io.simplesource.kafka.internal.util.RetryDelay;
import io.simplesource.kafka.spec.AggregateSpec;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;


import static io.simplesource.api.CommandError.Reason.*;
import static io.simplesource.kafka.api.AggregateResources.TopicEntity.command_request;

public final class KafkaCommandAPI<K, C, A> implements CommandAPI<K, C> {

    private final String aggregateName;
    private final String commandRequestTopic;
    private final Producer<K, CommandRequest<K, C>> commandProducer;
    private final AggregateSerdes<K, C, ?, A> aggregateSerdes;
    private final HostInfo currentHost;
    private final CommandResponseStoreBridge<A> storeBridge;
    private final RemoteCommandResponseStore remoteStore;
    private final ScheduledExecutorService scheduledExecutor;
    private final RetryDelay retryDelay;

    public KafkaCommandAPI(
        final AggregateSpec<K, C, ?, A> aggregateSpec,
        final KafkaConfig kafkaConfig,
        final CommandResponseStoreBridge<A> storeBridge,
        final RemoteCommandResponseStore remoteStore,
        final ScheduledExecutorService scheduledExecutor,
        final RetryDelay retryDelay
    ) {
        commandRequestTopic = aggregateSpec.serialization().resourceNamingStrategy().topicName(
            aggregateSpec.aggregateName(),
            command_request.name());
        AggregateSpec.Serialization<K, C, ?, A> serialization = aggregateSpec.serialization();
        aggregateSerdes = serialization.serdes();
        commandProducer = new KafkaProducer<>(
            kafkaConfig.producerConfig(),
            aggregateSerdes.aggregateKey().serializer(),
            aggregateSerdes.commandRequest().serializer());
        currentHost = kafkaConfig.currentHostInfo();
        this.aggregateName = aggregateSpec.aggregateName();
        this.storeBridge = storeBridge;
        this.remoteStore = remoteStore;
        this.scheduledExecutor = scheduledExecutor;
        this.retryDelay = retryDelay;
    }

    @Override
    public FutureResult<CommandError, UUID> publishCommand(final Request<K, C> request) {
        final CommandRequest<K, C> commandRequest = new CommandRequest<>(
                request.key(), request.command(), request.readSequence(), request.commandId());
        final ProducerRecord<K, CommandRequest<K, C>> record = new ProducerRecord<>(
            commandRequestTopic,
            request.key(),
            commandRequest);
        return FutureResult.ofFuture(commandProducer.send(record), e -> CommandError.of(CommandPublishError, e))
            .map(meta -> request.commandId());
    }

    @Override
    public FutureResult<CommandError, NonEmptyList<Sequence>> queryCommandResult(final UUID commandId, final Duration timeout) {
        return StateStoreUtils.get(
            hostInfoForCommandResponseKey(commandId),
            currentHost,
            () -> getLocalAggregate(commandId),
            (hostInfo, newTimeout) -> remoteStore.get(hostInfo, aggregateName, commandId, newTimeout),
            () -> CommandError.of(Timeout, "Request timed out"),
            e -> CommandError.of(RemoteLookupFailed, e),
            scheduledExecutor,
            retryDelay,
            timeout);
    }

    private Optional<Result<CommandError, NonEmptyList<Sequence>>> getLocalAggregate(final UUID commandId) {
        final ReadOnlyWindowStore<UUID, AggregateUpdateResult<A>> commandResponseStore = storeBridge.getCommandResponseStore();
        final WindowStoreIterator<AggregateUpdateResult<A>> iterator =
            commandResponseStore
                .fetch(
                    commandId,
                    0L,
                    System.currentTimeMillis());

        final Iterable<KeyValue<Long, AggregateUpdateResult<A>>> iterable = () -> iterator;
        final Optional<AggregateUpdateResult<A>> response = StreamSupport.stream(iterable.spliterator(), false)
            .max(Comparator.comparingLong(kv -> kv.key))
            .map(kv -> kv.value);

        return response.map(
            result -> result.updatedAggregateResult().map(
                aggregateUpdate -> {
                    final Sequence head = result.readSequence().next();
                    final List<Sequence> tail = new ArrayList<>();
                    for (Sequence seq = head.next(); seq.isLessThanOrEqual(aggregateUpdate.sequence()); seq = seq.next()) {
                        tail.add(seq);
                    }
                    return new NonEmptyList<>(head, tail);
                }));
    }

    private Supplier<Result<CommandError, HostInfo>> hostInfoForCommandResponseKey(final UUID key) {
        return () -> storeBridge.hostInfoForCommandResponseStoreKey(key)
            .map(Result::<CommandError, HostInfo>success)
            .orElse(Result.failure(CommandError.of(AggregateNotFound, "No metadata found for command response key " + key)));
    }
}
