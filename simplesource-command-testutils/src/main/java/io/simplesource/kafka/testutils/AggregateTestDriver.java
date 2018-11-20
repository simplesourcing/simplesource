package io.simplesource.kafka.testutils;

import io.simplesource.api.CommandAPI;
import io.simplesource.api.CommandError;
import io.simplesource.data.Sequence;
import io.simplesource.data.FutureResult;
import io.simplesource.kafka.api.AggregateResources;
import io.simplesource.kafka.api.AggregateSerdes;
import io.simplesource.kafka.dsl.KafkaConfig;
import io.simplesource.kafka.internal.client.ResponseSubscription;
import io.simplesource.kafka.internal.client.KafkaCommandAPI;
import io.simplesource.kafka.internal.client.KafkaRequestAPI;
import io.simplesource.kafka.internal.client.RequestPublisher;
import io.simplesource.kafka.internal.streams.topology.EventSourcedTopology;
import io.simplesource.kafka.internal.streams.topology.TopologyContext;
import io.simplesource.kafka.model.*;
import io.simplesource.kafka.spec.AggregateSpec;
import io.simplesource.kafka.spec.CommandSpec;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.*;

import java.time.Duration;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.StreamSupport;

import static io.simplesource.kafka.api.AggregateResources.StateStoreEntity;
import static io.simplesource.kafka.api.AggregateResources.TopicEntity;

public final class AggregateTestDriver<K, C, E, A> implements CommandAPI<K, C> {
    private final TopologyTestDriver driver;
    private final AggregateSpec<K, C, E, A> aggregateSpec;
    private final AggregateSerdes<K, C, E, A> aggregateSerdes;

    private final CommandAPI<K, C> commandAPI;
    private final ArrayList<Runnable> statePollers;

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

        // create a version of the command API that pipes stuff in and out of the TestTopologyDriver
        RequestPublisher<K, CommandRequest<K, C>> commandRequestPublisher =
                new TestPublisher<>(driver, aggregateSerdes.aggregateKey(), aggregateSerdes.commandRequest(), topicName(TopicEntity.command_request));
        final RequestPublisher<UUID, String> responseTopicMapPublisher =
                new TestPublisher<>(driver, aggregateSerdes.commandResponseKey(), Serdes.String(), topicName(TopicEntity.command_response_topic_map));

        CommandSpec<K, C> commandSpec = aggregateSpec.getCommandSpec("localhost");
        KafkaRequestAPI.RequestAPIContext<?, ?, CommandResponse> requestCtx = KafkaCommandAPI.getRequestAPIContext(commandSpec, kafkaConfig);
        
        TestTopologyReceiver.ReceiverSpec<UUID, CommandResponse> receiverSpec = new TestTopologyReceiver.ReceiverSpec<>(
                requestCtx.privateResponseTopic(), 400, 4,
                requestCtx.responseValueSerde(),
                stringKey -> UUID.fromString(stringKey.substring(stringKey.length() - 36)));

        statePollers = new ArrayList<>();
        final Function<BiConsumer<UUID, CommandResponse>, ResponseSubscription> receiverAttacher = updateTarget -> {
            TestTopologyReceiver<UUID, CommandResponse> receiver = new TestTopologyReceiver<>(updateTarget, driver, receiverSpec);
            statePollers.add(receiver::pollForState);
            return receiver;
        };

        commandAPI = new KafkaCommandAPI<>(commandSpec,
                kafkaConfig,
                commandRequestPublisher,
                responseTopicMapPublisher,
                receiverAttacher);
    }

    @Override
    public FutureResult<CommandError, UUID> publishCommand(final Request<K, C> request) {
        return commandAPI.publishCommand(request);
    }

    @Override
    public FutureResult<CommandError, Sequence> queryCommandResult(
        final UUID commandId,
        final Duration timeout) {
        commandAPI.queryCommandResult(commandId, timeout);
        pollForApiResponse();
        return commandAPI.queryCommandResult(commandId, timeout);
    }

    Optional<KeyValue<K, AggregateUpdate<A>>> readAggregateTopic() {
        final ProducerRecord<K, AggregateUpdate<A>> maybeRecord = driver.readOutput(
                topicName(TopicEntity.aggregate),
                aggregateSerdes.aggregateKey().deserializer(),
                aggregateSerdes.aggregateUpdate().deserializer()
        );
        return Optional.ofNullable(maybeRecord)
                .map(record -> KeyValue.pair(
                        record.key(),
                        record.value()));
    }

    Optional<KeyValue<K, CommandResponse>> readCommandResponseTopic() {
        final ProducerRecord<K, CommandResponse> maybeRecord = driver.readOutput(
                topicName(TopicEntity.command_response),
                aggregateSerdes.aggregateKey().deserializer(),
                aggregateSerdes.commandResponse().deserializer()
        );
        return Optional.ofNullable(maybeRecord)
                .map(record -> KeyValue.pair(
                        record.key(),
                        record.value()));
    }

    Optional<KeyValue<K, ValueWithSequence<E>>> readEventTopic() {
        final ProducerRecord<K, ValueWithSequence<E>> maybeRecord = driver.readOutput(
            topicName(TopicEntity.event),
                aggregateSerdes.aggregateKey().deserializer(),
            aggregateSerdes.valueWithSequence().deserializer()
        );
        return Optional.ofNullable(maybeRecord)
            .map(record -> KeyValue.pair(
                record.key(),
                record.value()));
    }

    public void pollForApiResponse() {
        statePollers.forEach(Runnable::run);
    }

    public void close() {
        if (driver != null) {
            driver.close();
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
