package io.simplesource.kafka.internal.streams;

import io.simplesource.api.CommandAPI;
import io.simplesource.api.CommandError;
import io.simplesource.data.FutureResult;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Sequence;
import io.simplesource.kafka.api.AggregateResources;
import io.simplesource.kafka.api.AggregateSerdes;
import io.simplesource.kafka.dsl.KafkaConfig;
import io.simplesource.kafka.internal.KafkaCommandAPI;
import io.simplesource.kafka.internal.streams.statestore.AggregateStoreBridge;
import io.simplesource.kafka.internal.streams.statestore.CommandResponseStoreBridge;
import io.simplesource.kafka.internal.streams.topologies.TopologyContext;
import io.simplesource.kafka.internal.streams.topologies.EventSourcedTopology;
import io.simplesource.kafka.internal.util.NamedThreadFactory;
import io.simplesource.kafka.internal.util.RetryDelay;
import io.simplesource.kafka.model.AggregateUpdate;
import io.simplesource.kafka.model.AggregateUpdateResult;
import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.kafka.model.ValueWithSequence;
import io.simplesource.kafka.spec.AggregateSpec;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.*;
import org.apache.kafka.streams.test.ConsumerRecordFactory;

import java.time.Duration;
import java.util.Comparator;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.StreamSupport;

import static io.simplesource.kafka.api.AggregateResources.StateStoreEntity.aggregate_update;
import static io.simplesource.kafka.api.AggregateResources.StateStoreEntity.command_response;
import static io.simplesource.kafka.api.AggregateResources.TopicEntity.*;

public final class AggregateTestDriver<K, C, E, A> implements CommandAPI<K, C> {
    private final TopologyTestDriver driver;
    private final AggregateSpec<K, C, E, A> aggregateSpec;
    private final KafkaConfig kafkaConfig;
    private final AggregateSerdes<K, C, E, A> aggregateSerdes;
    private final TestDriverPublisher publisher;

    private final CommandAPI<K, C> commandAPI;

    public AggregateTestDriver(
        final AggregateSpec<K, C, E, A> aggregateSpec,
        final KafkaConfig kafkaConfig
    ) {
        final StreamsBuilder builder = new StreamsBuilder();
        final EventSourcedTopology<K, C, E, A> topology = new EventSourcedTopology<>(new TopologyContext<>(aggregateSpec));
        topology.addTopology(builder);
        final TestDriverStoreBridge storeBridge = new TestDriverStoreBridge();
        final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor(
            new NamedThreadFactory("QueryAPI-scheduler"));
        final RetryDelay retryDelay = (startTime, timeoutMillis, spinCount) -> 15L;

        this.aggregateSpec = aggregateSpec;
        this.kafkaConfig = kafkaConfig;
        aggregateSerdes = aggregateSpec.serialization().serdes();
        final Properties streamConfig = new Properties();
        streamConfig.putAll(kafkaConfig.streamsConfig());
        driver = new TopologyTestDriver(builder.build(), streamConfig, 0L);
        publisher = new TestDriverPublisher(aggregateSerdes);
        commandAPI = new KafkaCommandAPI<>(
            aggregateSpec,
            kafkaConfig,
            storeBridge,
            null,
            scheduledExecutor,
            retryDelay);
    }

    @Override
    public FutureResult<CommandError, UUID> publishCommand(final Request<K, C> request) {
        publisher.publish(topicName(command_request), request.key(), new CommandRequest<>(request.command(), request.readSequence(), request.commandId()));
        return FutureResult.of(request.commandId());
    }

    @Override
    public FutureResult<CommandError, NonEmptyList<Sequence>> queryCommandResult(
        final UUID commandId,
        final Duration timeout) {
        return commandAPI.queryCommandResult(commandId, timeout);
    }

    Optional<KeyValue<K, AggregateUpdate<A>>> readAggregateTopic() {
        final ProducerRecord<K, AggregateUpdate<A>> maybeRecord = driver.readOutput(
            topicName(aggregate),
            aggregateSerdes.aggregateKey().deserializer(),
            aggregateSerdes.aggregateUpdate().deserializer()
        );
        return Optional.ofNullable(maybeRecord)
            .map(record -> KeyValue.pair(
                record.key(),
                record.value()));
    }

    Optional<KeyValue<K, ValueWithSequence<E>>> readEventTopic() {
        final ProducerRecord<K, ValueWithSequence<E>> maybeRecord = driver.readOutput(
            topicName(event),
                aggregateSerdes.aggregateKey().deserializer(),
            aggregateSerdes.valueWithSequence().deserializer()
        );
        return Optional.ofNullable(maybeRecord)
            .map(record -> KeyValue.pair(
                record.key(),
                record.value()));
    }

    Optional<AggregateUpdateResult<A>> fetchAggregateUpdateResult(UUID key) {
        final WindowStore<UUID, AggregateUpdateResult<A>> windowStore = driver.getWindowStore(storeName(command_response));
        final WindowStoreIterator<AggregateUpdateResult<A>> iterator = windowStore
            .fetch(
                key,
                0L,
                System.currentTimeMillis());
        final Iterable<KeyValue<Long, AggregateUpdateResult<A>>> iterable = () -> iterator;
        return StreamSupport.stream(iterable.spliterator(), false)
            .max(Comparator.comparingLong(kv -> kv.key))
            .map(kv -> kv.value);
    }

    public void close() {
        if (driver != null) {
            driver.close();
        }
    }

    private class TestDriverPublisher {
        private final ConsumerRecordFactory<K, CommandRequest<C>> factory;

        TestDriverPublisher(final AggregateSerdes<K, C, E, A> aggregateSerdes) {
            factory = new ConsumerRecordFactory<>(aggregateSerdes.aggregateKey().serializer(), aggregateSerdes.commandRequest().serializer());
        }

        private ConsumerRecordFactory<K, CommandRequest<C>> recordFactory() {
            return factory;
        }

        void publish(final String topic, final K key, final CommandRequest<C> value) {
            driver.pipeInput(recordFactory().create(topic, key, value));
        }

    }

    private class TestDriverStoreBridge implements AggregateStoreBridge<K, A>, CommandResponseStoreBridge<A> {

        @Override
        public ReadOnlyKeyValueStore<K, AggregateUpdate<A>> getAggregateStateStore() {
            return driver.getKeyValueStore(storeName(aggregate_update));
        }

        @Override
        public ReadOnlyWindowStore<UUID, AggregateUpdateResult<A>> getCommandResponseStore() {
            return driver.getWindowStore(storeName(command_response));
        }

        @Override
        public Optional<HostInfo> hostInfoForAggregateStoreKey(final K key) {
            return Optional.of(kafkaConfig.currentHostInfo());
        }

        @Override
        public Optional<HostInfo> hostInfoForCommandResponseStoreKey(final UUID key) {
            return Optional.of(kafkaConfig.currentHostInfo());
        }
    }

    private String topicName(final AggregateResources.TopicEntity topic) {
        return aggregateSpec.serialization().resourceNamingStrategy().topicName(
            aggregateSpec.aggregateName(), topic.name());
    }

    private String storeName(final AggregateResources.StateStoreEntity store) {
        return aggregateSpec.serialization().resourceNamingStrategy().storeName(
            aggregateSpec.aggregateName(), store.name());
    }

}
