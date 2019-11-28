package io.simplesource.kafka.testutils;

import io.simplesource.api.CommandAPI;
import io.simplesource.api.CommandError;
import io.simplesource.api.CommandId;
import io.simplesource.data.FutureResult;
import io.simplesource.data.Sequence;
import io.simplesource.kafka.api.AggregateResources;
import io.simplesource.kafka.api.AggregateSerdes;
import io.simplesource.kafka.dsl.KafkaConfig;
import io.simplesource.kafka.internal.client.*;
import io.simplesource.kafka.internal.streams.topology.EventSourcedTopology;
import io.simplesource.kafka.internal.streams.topology.TopologyContext;
import io.simplesource.kafka.internal.util.NamedThreadFactory;
import io.simplesource.kafka.model.AggregateUpdate;
import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.kafka.model.CommandResponse;
import io.simplesource.kafka.model.ValueWithSequence;
import io.simplesource.kafka.spec.AggregateSpec;
import io.simplesource.kafka.spec.CommandSpec;
import io.simplesource.kafka.util.SpecUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.simplesource.kafka.api.AggregateResources.TopicEntity;

public final class AggregateTestDriver<K, C, E, A> {
    private final TopologyTestDriver driver;
    private final AggregateSpec<K, C, E, A> aggregateSpec;
    private final AggregateSerdes<K, C, E, A> aggregateSerdes;

    private final KafkaCommandAPI<K, C> commandAPI;
    private final ArrayList<Runnable> statePollers;

    public AggregateTestDriver(
            final AggregateSpec<K, C, E, A> aggregateSpec,
            final KafkaConfig kafkaConfig
    ) {
        final StreamsBuilder builder = new StreamsBuilder();
        final TopologyContext<K, C, E, A> ctx = new TopologyContext<>(aggregateSpec);
        final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor(
                new NamedThreadFactory("QueryAPI-scheduler"));

        EventSourcedTopology.addTopology(ctx, builder);

        this.aggregateSpec = aggregateSpec;
        aggregateSerdes = aggregateSpec.serialization().serdes();
        final Properties streamConfig = new Properties();
        streamConfig.putAll(kafkaConfig.streamsConfig());
        driver = new TopologyTestDriver(builder.build(), streamConfig, 0L);

        // create a version of the command API that pipes stuff in and out of the TestTopologyDriver
        RequestPublisher<K, CommandRequest<K, C>> commandRequestPublisher =
                new TestPublisher<>(driver, aggregateSerdes.aggregateKey(), aggregateSerdes.commandRequest(), topicName(TopicEntity.COMMAND_REQUEST));
        final RequestPublisher<CommandId, String> responseTopicMapPublisher =
                new TestPublisher<>(driver, aggregateSerdes.commandId(), Serdes.String(), topicName(TopicEntity.COMMAND_RESPONSE_TOPIC_MAP));

        CommandSpec<K, C> commandSpec = SpecUtils.getCommandSpec(aggregateSpec,"localhost");
        RequestAPIContext<?, ?, CommandId, CommandResponse<K>> requestCtx =
                KafkaCommandAPI.getRequestAPIContext(commandSpec, kafkaConfig, scheduledExecutor);
        
        TestTopologyReceiver.ReceiverSpec<CommandId, CommandResponse<K>> receiverSpec = new TestTopologyReceiver.ReceiverSpec<>(
                requestCtx.privateResponseTopic(), 400, 4,
                requestCtx.responseValueSerde(),
                stringKey -> CommandId.of(UUID.fromString(stringKey.substring(stringKey.length() - 36))));

        statePollers = new ArrayList<>();
        final Function<BiConsumer<CommandId, CommandResponse<K>>, ResponseSubscription> receiverAttacher = updateTarget -> {
            TestTopologyReceiver<CommandId, CommandResponse<K>> receiver = new TestTopologyReceiver<CommandId, CommandResponse<K>>(updateTarget, driver, receiverSpec);
            statePollers.add(receiver::pollForState);
            return receiver;
        };

        commandAPI = new KafkaCommandAPI<>(commandSpec,
                kafkaConfig,
                scheduledExecutor,
                commandRequestPublisher,
                responseTopicMapPublisher,
                receiverAttacher);
    }

    public FutureResult<CommandError, CommandId> publishCommand(final CommandAPI.Request<K, C> request) {
        return commandAPI.publishCommand(request);
    }

    public FutureResult<CommandError, Sequence> queryCommandResult(
        final CommandId commandId,
        final Duration timeout) {
        pollForAPIResponse();
        return commandAPI.queryCommandResult(commandId, timeout);
    }

    Optional<KeyValue<K, AggregateUpdate<A>>> readAggregateTopic() {
        final ProducerRecord<K, AggregateUpdate<A>> maybeRecord = driver.readOutput(
                topicName(TopicEntity.AGGREGATE),
                aggregateSerdes.aggregateKey().deserializer(),
                aggregateSerdes.aggregateUpdate().deserializer()
        );
        return Optional.ofNullable(maybeRecord)
                .map(record -> KeyValue.pair(
                        record.key(),
                        record.value()));
    }

    Optional<KeyValue<K, CommandResponse<K>>> readCommandResponseTopic() {
        final ProducerRecord<K, CommandResponse<K>> maybeRecord = driver.readOutput(
                topicName(TopicEntity.COMMAND_RESPONSE),
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
            topicName(TopicEntity.EVENT),
                aggregateSerdes.aggregateKey().deserializer(),
            aggregateSerdes.valueWithSequence().deserializer()
        );
        return Optional.ofNullable(maybeRecord)
            .map(record -> KeyValue.pair(
                record.key(),
                record.value()));
    }

    public void pollForAPIResponse() {
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
}
