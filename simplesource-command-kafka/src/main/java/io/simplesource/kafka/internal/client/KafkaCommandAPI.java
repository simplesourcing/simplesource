package io.simplesource.kafka.internal.client;

import lombok.Value;
import avro.shaded.com.google.common.collect.Lists;
import io.simplesource.api.CommandAPI;
import io.simplesource.api.CommandError;
import io.simplesource.data.*;
import io.simplesource.kafka.api.CommandSerdes;
import io.simplesource.kafka.dsl.KafkaConfig;
import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.kafka.model.CommandResponse;
import io.simplesource.kafka.spec.CommandSpec;
import io.simplesource.kafka.spec.TopicSpec;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.HostInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;


import static io.simplesource.api.CommandError.Reason.*;
import static io.simplesource.kafka.api.AggregateResources.TopicEntity.command_request;
import static io.simplesource.kafka.api.AggregateResources.TopicEntity.command_response;
import static io.simplesource.kafka.api.AggregateResources.TopicEntity.command_response_topic_map;


public final class KafkaCommandAPI<K, C> implements CommandAPI<K, C> {
    private static final Logger logger = LoggerFactory.getLogger(KafkaCommandAPI.class);

    @Value
    static final class ResponseHandlers {
        final List<CompletableFuture<CommandResponse>> handlers;
    }

    private final String commandRequestTopic;
    private final String commandResponseTopicMapTopic;
    private final String commandResponseTopicName;
    private final Producer<K, CommandRequest<K, C>> commandProducer;
    private final Producer<UUID, String> responseTopicMapProducer;
    private final KafkaConsumerRunner consumerRunner;
    private final ConcurrentHashMap<UUID, ResponseHandlers> handlerMap = new ConcurrentHashMap<>();

    public KafkaCommandAPI(
        final CommandSpec<K, C> commandSpec,
        final KafkaConfig kafkaConfig
    ) {
        commandRequestTopic = commandSpec.resourceNamingStrategy().topicName(
            commandSpec.aggregateName(),
            command_request.name());
        commandResponseTopicMapTopic = commandSpec.resourceNamingStrategy().topicName(
                commandSpec.aggregateName(),
                command_response_topic_map.name());
        String commandResponseTopicBase = commandSpec.resourceNamingStrategy().topicName(
                commandSpec.aggregateName(),
                command_response.name());
        CommandSerdes<K, C> commandSerdes = commandSpec.serdes();
        commandProducer = new KafkaProducer<>(
            kafkaConfig.producerConfig(),
                commandSerdes.aggregateKey().serializer(),
                commandSerdes.commandRequest().serializer());
        responseTopicMapProducer = new KafkaProducer<>(
                kafkaConfig.producerConfig(),
                commandSerdes.commandResponseKey().serializer(),
                Serdes.String().serializer()
        );
        HostInfo currentHost = kafkaConfig.currentHostInfo();
        this.commandResponseTopicName = String.format("%s_%s_%d", commandResponseTopicBase, currentHost.host(), currentHost.port());

        AdminClient adminClient = AdminClient.create(kafkaConfig.adminClientConfig());

        try {
            Set<String> topics = adminClient.listTopics().names().get();
            if (!topics.contains(commandResponseTopicName)) {
                    TopicSpec config = commandSpec.outputTopicConfig();
                    NewTopic newTopic = new NewTopic(commandResponseTopicName, config.partitionCount(), config.replicaCount());
                    adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
                }
            } catch (Exception e) {
            throw new RuntimeException("Unable to create", e);
        }

        consumerRunner = new KafkaConsumerRunner(kafkaConfig.producerConfig(), commandResponseTopicName, commandSpec, handlerMap);
        new Thread(consumerRunner).start();

        Runtime.getRuntime().addShutdownHook(
                new Thread(
                        () -> {
                            logger.info("CommandAPI shutting down");
                            consumerRunner.close();
                        }
                )
        );

    }

    @Override
    public FutureResult<CommandError, UUID> publishCommand(final Request<K, C> request) {
        final CommandRequest<K, C> commandRequest = new CommandRequest<>(
                request.key(), request.command(), request.readSequence(), request.commandId());
        final ProducerRecord<K, CommandRequest<K, C>> record = new ProducerRecord<>(
                commandRequestTopic,
                request.key(),
                commandRequest);

        final ProducerRecord<UUID, String> responseTopicRecord = new ProducerRecord<>(
                commandResponseTopicMapTopic,
                request.commandId(),
                commandResponseTopicName);

        return FutureResult.ofFuture(
                responseTopicMapProducer.send(responseTopicRecord), e -> CommandError.of(CommandPublishError, e))
                .flatMap(
                    metaData ->
                        FutureResult.ofFuture(commandProducer.send(record), e -> CommandError.of(CommandPublishError, e)))
                .map(meta -> {
                    UUID commandId = request.commandId();
                    handlerMap.computeIfAbsent(commandId, uuid -> new ResponseHandlers(Lists.newArrayList()));
                    return commandId;
                });
    }

    @Override
    public FutureResult<CommandError, NonEmptyList<Sequence>> queryCommandResult(final UUID commandId, final Duration timeout) {
        CompletableFuture<CommandResponse> completableFuture = new CompletableFuture<>();

        ResponseHandlers h = handlerMap.computeIfPresent(commandId, (id, handlers) -> {
            handlers.handlers.add(completableFuture);
            return handlers;
        });
        if (h == null) {
            completableFuture.completeExceptionally(new Exception("Invalid commandId."));
        }
        return FutureResult.ofCompletableFuture(completableFuture.thenApply(resp -> resp.sequenceResult().map(s -> NonEmptyList.of(s))));
    }

    @Override
    public void close() {
        this.consumerRunner.close();
    }
}

