package io.simplesource.kafka.internal;

import avro.shaded.com.google.common.collect.Lists;
import io.simplesource.api.CommandAPI;
import io.simplesource.api.CommandError;
import io.simplesource.data.*;
import io.simplesource.kafka.api.CommandSerdes;
import io.simplesource.kafka.api.RemoteCommandResponseStore;
import io.simplesource.kafka.dsl.KafkaConfig;
import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.kafka.internal.streams.statestore.CommandResponseStoreBridge;
import io.simplesource.kafka.internal.streams.statestore.StateStoreUtils;
import io.simplesource.kafka.internal.util.RetryDelay;
import io.simplesource.kafka.model.CommandResponse;
import io.simplesource.kafka.spec.CommandSpec;
import io.simplesource.kafka.spec.TopicSpec;
import lombok.Value;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;


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

    private final String aggregateName;
    private final String commandRequestTopic;
    private final String commandResponseTopicMapTopic;
    private final String commandResponseTopicName;
    private final Producer<K, CommandRequest<K, C>> commandProducer;
    private final Producer<UUID, String> responseTopicMapProducer;
    private final HostInfo currentHost;
    private final CommandResponseStoreBridge storeBridge;
    private final RemoteCommandResponseStore remoteStore;
    private final ScheduledExecutorService scheduledExecutor;
    private final RetryDelay retryDelay;
    private final KafkaConsumerRunner consumerRunner;
    private final ConcurrentHashMap<UUID, ResponseHandlers> handlerMap = new ConcurrentHashMap<>();

    public KafkaCommandAPI(
        final CommandSpec<K, C> commandSpec,
        final KafkaConfig kafkaConfig,
        final CommandResponseStoreBridge storeBridge,
        final RemoteCommandResponseStore remoteStore,
        final ScheduledExecutorService scheduledExecutor,
        final RetryDelay retryDelay
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
        currentHost = kafkaConfig.currentHostInfo();
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

        this.aggregateName = commandSpec.aggregateName();
        this.storeBridge = storeBridge;
        this.remoteStore = remoteStore;
        this.scheduledExecutor = scheduledExecutor;
        this.retryDelay = retryDelay;

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
                    if (!handlerMap.containsKey(request.commandId())) {
                        handlerMap.put(request.commandId(), new ResponseHandlers(Lists.newArrayList()));
                    }
                    return request.commandId();
                });
    }

    @Override
    public FutureResult<CommandError, NonEmptyList<Sequence>> queryCommandResult(final UUID commandId, final Duration timeout) {
        ResponseHandlers handlers = handlerMap.get(commandId);
        CompletableFuture<CommandResponse> completableFuture = new CompletableFuture<>();
        handlers.handlers.add(completableFuture);
        return FutureResult.ofCompletableFuture(completableFuture.thenApply(resp -> resp.sequenceResult().map(s -> NonEmptyList.of(s))));
    }
}

class KafkaConsumerRunner implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerRunner.class);

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final KafkaConsumer<String, CommandResponse> consumer;
    private final String topicName;
    private final ConcurrentHashMap<UUID, KafkaCommandAPI.ResponseHandlers> handlerMap;

    private Properties copyProperties(Map<String, Object> properties) {
        Properties newProps = new Properties();
        properties.forEach((key, value) -> newProps.setProperty(key, value.toString()));
        return newProps;
    }

    KafkaConsumerRunner(Map<String, Object> properties, String topicName, CommandSpec<?, ?> commandSpec, ConcurrentHashMap<UUID, KafkaCommandAPI.ResponseHandlers> handlerMap) {

        Properties consumerConfig  = copyProperties(properties);
        //consumerConfig.putAll(spec.config)
        consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, String.format("response_consumer_%s", UUID.randomUUID().toString().substring(0, 8)));
        // For now automatic - probably rather do this manually
        consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumerConfig.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        consumerConfig.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        this.topicName = topicName;
        this.handlerMap = handlerMap;
        consumer = new KafkaConsumer<>(consumerConfig, Serdes.String().deserializer(), commandSpec.serdes().commandResponse().deserializer());
    }

    public void run() {
        try {
            consumer.subscribe(Collections.singletonList(topicName));
            while (!closed.get()) {
                ConsumerRecords<String, CommandResponse> records = consumer.poll(10000);
                // Handle new records
                records.iterator().forEachRemaining( record -> {
                    String recordKey = record.key();
                    UUID id = UUID.fromString(record.key().substring(recordKey.length() - 36));
                    KafkaCommandAPI.ResponseHandlers responseHandler = handlerMap.getOrDefault(id, null);
                    if (responseHandler != null) {
                        responseHandler.handlers().forEach(future -> {
                            future.complete(record.value());
                        });
                    }
                });
            }
        } catch (WakeupException e) {
            // Ignore exception if closing
            if (!closed.get()) throw e;
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            consumer.close();
        }
    }

    // Shutdown hook which can be called from a separate thread
    void close() {
        closed.set(true);
        consumer.wakeup();
    }
}
