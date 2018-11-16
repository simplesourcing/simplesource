package io.simplesource.kafka.internal.streams;

import io.simplesource.api.CommandAPI;
import io.simplesource.api.CommandError;
import io.simplesource.data.Sequence;
import io.simplesource.data.FutureResult;
import io.simplesource.data.NonEmptyList;
import io.simplesource.kafka.api.AggregateResources;
import io.simplesource.kafka.api.AggregateSerdes;
import io.simplesource.kafka.dsl.KafkaConfig;
import io.simplesource.kafka.internal.client.Closeable;
import io.simplesource.kafka.internal.client.KafkaCommandAPI;
import io.simplesource.kafka.internal.client.RequestSender;
import io.simplesource.kafka.internal.streams.topology.EventSourcedTopology;
import io.simplesource.kafka.internal.streams.topology.TopologyContext;
import io.simplesource.kafka.model.*;
import io.simplesource.kafka.spec.AggregateSpec;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.*;
import org.apache.kafka.streams.test.ConsumerRecordFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.StreamSupport;

import static io.simplesource.kafka.api.AggregateResources.StateStoreEntity.command_response;
import static io.simplesource.kafka.api.AggregateResources.TopicEntity.command_request;
import static io.simplesource.kafka.api.AggregateResources.TopicEntity.event;
import static io.simplesource.kafka.api.AggregateResources.TopicEntity.aggregate;


public class TestPublisher<K, V> implements RequestSender<K, V> {

    private final ConsumerRecordFactory<K,V> factory;
    TopologyTestDriver driver;
    private final String topicName;

    TestPublisher(TopologyTestDriver driver, final Serde<K> keySerde, final Serde<V> valueSerde, String topicName) {

        this.driver = driver;
        this.topicName = topicName;
        factory = new ConsumerRecordFactory<>(keySerde.serializer(), valueSerde.serializer());
    }

    @Override
    public FutureResult<Exception, SendResult> send(K key, V value) {

        driver.close();
        driver.pipeInput(factory.create(topicName, key, value));
        return FutureResult.of(new SendResult(Instant.now().getEpochSecond()));
    }
}


public final class AggregateTestDriver<K, C, E, A> implements CommandAPI<K, C> {
    private final TopologyTestDriver driver;
    private final AggregateSpec<K, C, E, A> aggregateSpec;
    private final AggregateSerdes<K, C, E, A> aggregateSerdes;
    private final TestDriverPublisher publisher;

    private final CommandAPI<K, C> commandAPI;

    public AggregateTestDriver(
            final AggregateSpec<K, C, E, A> aggregateSpec,
            final KafkaConfig kafkaConfig
    ) {
        final StreamsBuilder builder = new StreamsBuilder();
        final TopologyContext<K, C, E, A> ctx = new TopologyContext<>(aggregateSpec);
        EventSourcedTopology.addTopology(ctx, builder);

        this.aggregateSpec = aggregateSpec;
        aggregateSerdes = aggregateSpec.serialization().serdes();
        final Properties streamConfig = new Properties();
        streamConfig.putAll(kafkaConfig.streamsConfig());
        driver = new TopologyTestDriver(builder.build(), streamConfig, 0L);
        publisher = new TestDriverPublisher(aggregateSerdes);
        commandAPI = new KafkaCommandAPI<>(aggregateSpec.getCommandSpec(),
                kafkaConfig,
                new TestPublisher(),
                new TestPublisher(),
                null);
    }


    public AggregateTestDriver(
        final AggregateSpec<K, C, E, A> aggregateSpec,
            final KafkaConfig kafkaConfig,
        final RequestSender<K, CommandRequest<K, C>> requestSender,
        final RequestSender<UUID, String> responseTopicMapSender,
        final Function<BiConsumer<UUID, CommandResponse>, Closeable> attachReceiver
    ) {
        final StreamsBuilder builder = new StreamsBuilder();
        final TopologyContext<K, C, E, A> ctx = new TopologyContext<>(aggregateSpec);
        EventSourcedTopology.addTopology(ctx, builder);

        this.aggregateSpec = aggregateSpec;
        aggregateSerdes = aggregateSpec.serialization().serdes();
        final Properties streamConfig = new Properties();
        streamConfig.putAll(kafkaConfig.streamsConfig());
        driver = new TopologyTestDriver(builder.build(), streamConfig, 0L);
        publisher = new TestDriverPublisher(aggregateSerdes);
        commandAPI = new KafkaCommandAPI<>(aggregateSpec.getCommandSpec(),
                kafkaConfig,
                requestSender,
                responseTopicMapSender,
                attachReceiver);
    }

    @Override
    public FutureResult<CommandError, UUID> publishCommand(final Request<K, C> request) {
        publisher.publish(topicName(command_request), request.key(), new CommandRequest<>(
                request.key(),request.command(), request.readSequence(), request.commandId()));
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
        private final ConsumerRecordFactory<K, CommandRequest<K, C>> factory;

        TestDriverPublisher(final AggregateSerdes<K, C, E, A> aggregateSerdes) {
            factory = new ConsumerRecordFactory<>(aggregateSerdes.aggregateKey().serializer(), aggregateSerdes.commandRequest().serializer());
        }

        private ConsumerRecordFactory<K, CommandRequest<K, C>> recordFactory() {
            return factory;
        }

        void publish(final String topic, final K key, final CommandRequest<K, C> value) {
            driver.pipeInput(recordFactory().create(topic, key, value));
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
