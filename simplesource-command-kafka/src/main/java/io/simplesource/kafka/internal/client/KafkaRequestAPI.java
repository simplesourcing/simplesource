package io.simplesource.kafka.internal.client;

import avro.shaded.com.google.common.collect.Lists;
import io.simplesource.data.FutureResult;
import io.simplesource.kafka.dsl.KafkaConfig;
import io.simplesource.kafka.spec.TopicSpec;
import io.simplesource.kafka.spec.WindowSpec;
import lombok.Builder;
import lombok.Value;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;


public final class KafkaRequestAPI<K, I, O> {
    private static final Logger logger = LoggerFactory.getLogger(KafkaRequestAPI.class);

    @Value
    static final class ResponseReceiver<K, M, V> {
        final ExpiringMap<K, M> expiringMap;
        final BiFunction<M, V, M> mapModifier;

        void receive(K k, V v) {
            expiringMap.computeIfPresent(k, m -> mapModifier.apply(m, v));
        }
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    @Value
    static final class ResponseHandler<O> {
        final List<CompletableFuture<O>> responseFutures;
        final Optional<O> response;

        static <O> ResponseHandler<O> initialise(Optional<O> r) {
            return new ResponseHandler<>(Lists.newArrayList(), r);
        }

        void forEachFuture(Consumer<CompletableFuture<O>> action) {
            responseFutures.forEach(action);
        }
    }

    @Value
    @Builder
    public static final class RequestAPIContext<K, I, O> {
        final KafkaConfig kafkaConfig;
        final String requestTopic;
        final String responseTopicMapTopic;
        final String privateResponseTopic;
        final Serde<K> requestKeySerde;
        final Serde<I> requestValueSerde;
        final Serde<UUID> responseKeySerde;
        final Serde<O> responseValueSerde;
        final WindowSpec responseWindowSpec;
        final TopicSpec outputTopicConfig;
    }

    private final RequestAPIContext<K, I, O> ctx;
    private final ResponseSubscription responseSubscription;
    private final ExpiringMap<UUID, ResponseHandler<O>> responseHandlers;
    private final RequestPublisher<K, I> requestSender;
    private final RequestPublisher<UUID, String> responseTopicMapSender;

    private static <K, V> RequestPublisher<K, V> kakfaProducerSender(
            KafkaConfig kafkaConfig,
            String topicName,
            Serde<K> keySerde,
            Serde<V> valueSerde) {
        KafkaProducer<K, V> producer = new KafkaProducer<>(
                kafkaConfig.producerConfig(),
                keySerde.serializer(),
                valueSerde.serializer());
        return (key, value) -> {
            final ProducerRecord<K, V> record = new ProducerRecord<>(
                    topicName,
                    key,
                    value);
            return FutureResult.ofFuture(producer.send(record), e -> e)
                    .map(meta -> new RequestPublisher.PublishResult(meta.timestamp()));
        };
    }

    public KafkaRequestAPI(final RequestAPIContext<K, I, O> ctx) {
        this(ctx,
                kakfaProducerSender(ctx.kafkaConfig, ctx.requestTopic, ctx.requestKeySerde, ctx.requestValueSerde),
                kakfaProducerSender(ctx.kafkaConfig, ctx.responseTopicMapTopic, ctx.responseKeySerde, Serdes.String()),
                receiver -> KafkaConsumerRunner.run(
                    ctx.kafkaConfig().consumerConfig(),
                    ctx.privateResponseTopic(),
                    ctx.responseValueSerde(),
                    receiver),
                true);
    }

    public KafkaRequestAPI(
            final RequestAPIContext<K, I, O> ctx,
            final RequestPublisher<K, I> requestSender,
            final RequestPublisher<UUID, String> responseTopicMapSender,
            final Function<BiConsumer<UUID, O>, ResponseSubscription> responseSubscriber,
            boolean createTopics) {
        KafkaConfig kafkaConfig = ctx.kafkaConfig();

        this.ctx = ctx;
        long retentionInSeconds = ctx.responseWindowSpec().retentionInSeconds();
        this.requestSender = requestSender;
        this.responseTopicMapSender = responseTopicMapSender;

        if (createTopics) {
            AdminClient adminClient = AdminClient.create(kafkaConfig.adminClientConfig());
            try {
                Set<String> topics = adminClient.listTopics().names().get();
                String privateResponseTopic = ctx.privateResponseTopic;
                if (!topics.contains(privateResponseTopic)) {
                    TopicSpec topicSpec = ctx.outputTopicConfig();
                    NewTopic newTopic = new NewTopic(privateResponseTopic, topicSpec.partitionCount(), topicSpec.replicaCount());
                    adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
                }
            } catch (Exception e) {
                throw new RuntimeException("Unable to create required topics.", e);
            }
        }

        responseHandlers = new ExpiringMap<>(retentionInSeconds, Clock.systemUTC());
        ResponseReceiver<UUID, ResponseHandler<O>, O> responseReceiver =
            new ResponseReceiver<>(responseHandlers, (h, r) -> {
                h.forEachFuture(future -> future.complete(r));
                return ResponseHandler.initialise(Optional.of(r));
            });

        this.responseSubscription = responseSubscriber.apply(responseReceiver::receive);

        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
    }

    public FutureResult<Exception, RequestPublisher.PublishResult> publishRequest(final K key, UUID requestId, final I request) {

        FutureResult<Exception, RequestPublisher.PublishResult> result = responseTopicMapSender.publish(requestId, ctx.privateResponseTopic())
                .flatMap(r -> requestSender.publish(key, request)).map(r -> {
                    responseHandlers.insertIfAbsent(requestId, () -> ResponseHandler.initialise(Optional.empty()));
                    return r;
                });

        responseHandlers.removeStaleAsync(h ->
                h.forEachFuture(f ->
                        f.completeExceptionally(new Exception("Request timed out."))));

        return result;
    }

    public CompletableFuture<O> queryResponse(final UUID requestId, final Duration timeout) {
        // TODO - handle timeout...
        CompletableFuture<O> completableFuture = new CompletableFuture<>();
        ResponseHandler handler = responseHandlers.computeIfPresent(requestId, h -> {
            Optional<O> response = h.response;
            if (response.isPresent())
                completableFuture.complete(response.get());
            else
                h.responseFutures.add(completableFuture);
            return h;
        });
        if (handler == null) {
            completableFuture.completeExceptionally(new Exception("Invalid commandId."));
        }
        return completableFuture;
    }

    public void close() {
        logger.info("Request API shutting down");
        responseHandlers.removeAll(handlers ->
                handlers.forEachFuture(future ->
                        future.completeExceptionally(new Exception("Consumer closed before future."))));

        this.responseSubscription.close();
    }
}

