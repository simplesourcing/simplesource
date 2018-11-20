package io.simplesource.kafka.internal.client;

import io.simplesource.api.CommandAPI;
import io.simplesource.api.CommandError;
import io.simplesource.data.FutureResult;
import io.simplesource.data.Sequence;
import io.simplesource.kafka.api.CommandSerdes;
import io.simplesource.kafka.api.ResourceNamingStrategy;
import io.simplesource.kafka.dsl.KafkaConfig;
import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.kafka.model.CommandResponse;
import io.simplesource.kafka.spec.CommandSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.simplesource.kafka.api.AggregateResources.TopicEntity.*;

public final class KafkaCommandAPI<K, C> implements CommandAPI<K, C> {
    private static final Logger logger = LoggerFactory.getLogger(KafkaCommandAPI.class);

    private KafkaRequestAPI<K, CommandRequest<K, C>, CommandResponse> requestApi;

    public KafkaCommandAPI(
            final CommandSpec<K, C> commandSpec,
            final KafkaConfig kafkaConfig) {
        KafkaRequestAPI.RequestAPIContext<K, CommandRequest<K, C>, CommandResponse> ctx = getRequestAPIContext(commandSpec, kafkaConfig);
        requestApi = new KafkaRequestAPI<>(ctx);
    }

    public KafkaCommandAPI(
            final CommandSpec<K, C> commandSpec,
            final KafkaConfig kafkaConfig,
            final RequestPublisher<K, CommandRequest<K, C>> requestSender,
            final RequestPublisher<UUID, String> responseTopicMapSender,
            final Function<BiConsumer<UUID, CommandResponse>, ResponseSubscription> attachReceiver) {

        KafkaRequestAPI.RequestAPIContext<K, CommandRequest<K, C>, CommandResponse> ctx = getRequestAPIContext(commandSpec, kafkaConfig);
        requestApi = new KafkaRequestAPI<>(ctx, requestSender, responseTopicMapSender, attachReceiver, false);
    }

    @Override
    public FutureResult<CommandError, UUID> publishCommand(final Request<K, C> request) {
        final CommandRequest<K, C> commandRequest = new CommandRequest<>(
                request.key(), request.command(), request.readSequence(), request.commandId());

        FutureResult<Exception, RequestPublisher.PublishResult> publishResult = requestApi.publishRequest(request.key(), request.commandId(), commandRequest);

        // A lot of trouble to change the error type from Exception to CommandError
        Future<FutureResult<CommandError, UUID>> futureOfFR = publishResult.fold(
                errors -> FutureResult.fail(CommandError.of(CommandError.Reason.CommandPublishError, errors.head())),
                r -> FutureResult.of(request.commandId()));

        return FutureResult
                .ofFuture(futureOfFR, e -> {
                    logger.debug("Error in publishing command", e);
                    Throwable rootCause = Optional.ofNullable(e.getCause()).orElse(e);
                    return CommandError.of(CommandError.Reason.CommandPublishError, rootCause);
                })
                .flatMap(y -> y);
    }

    @Override
    public FutureResult<CommandError, Sequence> queryCommandResult(final UUID commandId, final Duration timeout) {
        CompletableFuture<CommandResponse> completableFuture = requestApi.queryResponse(commandId, timeout);
        return FutureResult.ofCompletableFuture(completableFuture.thenApply(CommandResponse::sequenceResult));
    }

    public static <K, C> KafkaRequestAPI.RequestAPIContext<K, CommandRequest<K, C>, CommandResponse> getRequestAPIContext(CommandSpec<K, C> commandSpec, KafkaConfig kafkaConfig) {
        ResourceNamingStrategy namingStrategy = commandSpec.resourceNamingStrategy();
        CommandSerdes<K, C> serdes = commandSpec.serdes();
        String responseTopicBase = namingStrategy.topicName(
                commandSpec.aggregateName(),
                command_response.name());

        String privateResponseTopic =  String.format("%s_%s", responseTopicBase, commandSpec.clientId());
        return KafkaRequestAPI.RequestAPIContext.<K, CommandRequest<K, C>, CommandResponse>builder()
                .kafkaConfig(kafkaConfig)
                .requestTopic(namingStrategy.topicName(
                        commandSpec.aggregateName(),
                        command_request.name()))
                .responseTopicMapTopic(namingStrategy.topicName(
                        commandSpec.aggregateName(),
                        command_response_topic_map.name()))
                .privateResponseTopic(privateResponseTopic)
                .requestKeySerde(serdes.aggregateKey())
                .requestValueSerde(serdes.commandRequest())
                .responseKeySerde(serdes.commandResponseKey())
                .responseValueSerde(serdes.commandResponse())
                .responseWindowSpec(commandSpec.commandResponseWindowSpec())
                .outputTopicConfig(commandSpec.outputTopicConfig())
                .build();
    }
}

