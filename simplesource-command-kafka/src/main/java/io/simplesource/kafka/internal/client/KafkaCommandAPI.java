package io.simplesource.kafka.internal.client;

import io.simplesource.api.CommandAPI;
import io.simplesource.api.CommandError;
import io.simplesource.data.FutureResult;
import io.simplesource.data.Result;
import io.simplesource.data.Sequence;
import io.simplesource.kafka.api.CommandSerdes;
import io.simplesource.kafka.api.ResourceNamingStrategy;
import io.simplesource.kafka.dsl.KafkaConfig;
import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.kafka.model.CommandResponse;
import io.simplesource.kafka.spec.CommandSpec;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.simplesource.kafka.api.AggregateResources.TopicEntity.*;

public final class KafkaCommandAPI<K, C> implements CommandAPI<K, C> {

    private KafkaRequestAPI<K, CommandRequest<K, C>, CommandResponse> requestApi;

    public KafkaCommandAPI(
            final CommandSpec<K, C> commandSpec,
            final KafkaConfig kafkaConfig,
            final ScheduledExecutorService scheduler) {
        RequestAPIContext<K, CommandRequest<K, C>, CommandResponse> ctx = getRequestAPIContext(
                commandSpec,
                kafkaConfig,
                scheduler);
        requestApi = new KafkaRequestAPI<>(ctx);
    }

    public KafkaCommandAPI(
            final CommandSpec<K, C> commandSpec,
            final KafkaConfig kafkaConfig,
            final ScheduledExecutorService scheduler,
            final RequestPublisher<K, CommandRequest<K, C>> requestSender,
            final RequestPublisher<UUID, String> responseTopicMapSender,
            final Function<BiConsumer<UUID, CommandResponse>, ResponseSubscription> attachReceiver) {

        RequestAPIContext<K, CommandRequest<K, C>, CommandResponse> ctx = getRequestAPIContext(
                commandSpec,
                kafkaConfig,
                scheduler);
        requestApi = new KafkaRequestAPI<>(ctx, requestSender, responseTopicMapSender, attachReceiver, false);
    }

    private static CommandError getCommandError(Throwable e) {
        if (e instanceof TimeoutException)
            return CommandError.of(CommandError.Reason.Timeout, e);
        return CommandError.of(CommandError.Reason.CommandPublishError, e);
    }

    @Override
    public FutureResult<CommandError, UUID> publishCommand(final Request<K, C> request) {
        final CommandRequest<K, C> commandRequest = new CommandRequest<>(
                request.key(), request.command(), request.readSequence(), request.commandId());

        FutureResult<Exception, RequestPublisher.PublishResult> publishResult = requestApi.publishRequest(request.key(), request.commandId(), commandRequest);

        return publishResult.errorMap(KafkaCommandAPI::getCommandError)
                .map(r -> request.commandId());
    }

    @Override
    public FutureResult<CommandError, Sequence> queryCommandResult(final UUID commandId, final Duration timeout) {
        CompletableFuture<CommandResponse> completableFuture = requestApi.queryResponse(commandId, timeout);

        return FutureResult.ofCompletableFuture(completableFuture.thenApply(CommandResponse::sequenceResult));
    }

    public static <K, C> RequestAPIContext<K, CommandRequest<K, C>, CommandResponse> getRequestAPIContext(
            CommandSpec<K, C> commandSpec,
            KafkaConfig kafkaConfig,
            ScheduledExecutorService scheduler) {
        ResourceNamingStrategy namingStrategy = commandSpec.resourceNamingStrategy();
        CommandSerdes<K, C> serdes = commandSpec.serdes();
        String responseTopicBase = namingStrategy.topicName(
                commandSpec.aggregateName(),
                command_response.name());

        String privateResponseTopic =  String.format("%s_%s", responseTopicBase, commandSpec.clientId());
        return RequestAPIContext.<K, CommandRequest<K, C>, CommandResponse>builder()
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
                .scheduler(scheduler)
                .errorValue((i, e) ->
                        new CommandResponse(
                                i.commandId(),
                                i.readSequence(),
                                Result.failure(getCommandError(e))))
                .build();
    }
}

