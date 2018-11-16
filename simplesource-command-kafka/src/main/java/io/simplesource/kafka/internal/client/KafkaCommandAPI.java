package io.simplesource.kafka.internal.client;

import io.simplesource.api.CommandAPI;
import io.simplesource.api.CommandError;
import io.simplesource.data.FutureResult;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Sequence;
import io.simplesource.kafka.api.CommandSerdes;
import io.simplesource.kafka.api.ResourceNamingStrategy;
import io.simplesource.kafka.dsl.KafkaConfig;
import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.kafka.model.CommandResponse;
import io.simplesource.kafka.spec.CommandSpec;
import org.apache.kafka.streams.state.HostInfo;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.simplesource.kafka.api.AggregateResources.TopicEntity.*;

public final class KafkaCommandAPI<K, C> implements CommandAPI<K, C> {

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
            final RequestSender<K, CommandRequest<K, C>> requestSender,
            final RequestSender<UUID, String> responseTopicMapSender,
            final Function<BiConsumer<UUID, CommandResponse>, Closeable> attachReceiver) {

        KafkaRequestAPI.RequestAPIContext<K, CommandRequest<K, C>, CommandResponse> ctx = getRequestAPIContext(commandSpec, kafkaConfig);
        requestApi = new KafkaRequestAPI<>(ctx, requestSender, responseTopicMapSender, attachReceiver);
    }

    @Override
    public FutureResult<CommandError, UUID> publishCommand(final Request<K, C> request) {
        final CommandRequest<K, C> commandRequest = new CommandRequest<>(
                request.key(), request.command(), request.readSequence(), request.commandId());

        FutureResult<Exception, RequestSender.SendResult> publishResult = requestApi.publishRequest(request.key(), request.commandId(), commandRequest);

        // A lot of trouble to change the error type from Exception to CommandError
        Future<FutureResult<CommandError, UUID>> futureOfFR = publishResult.fold(
                errors -> FutureResult.fail(CommandError.of(CommandError.Reason.CommandPublishError, errors.head())),
                r -> FutureResult.of(request.commandId()));

        return FutureResult
                .ofFuture(futureOfFR, e -> CommandError.of(CommandError.Reason.CommandPublishError, "Publishing error"))
                .flatMap(y -> y);
    }

    @Override
    public FutureResult<CommandError, NonEmptyList<Sequence>> queryCommandResult(final UUID commandId, final Duration timeout) {
        CompletableFuture<CommandResponse> completableFuture = requestApi.queryResponse(commandId, timeout);
        return FutureResult.ofCompletableFuture(completableFuture.thenApply(resp -> resp.sequenceResult().map(s -> NonEmptyList.of(s))));
    }

    @Override
    public void close() {
        requestApi.close();
    }


    private KafkaRequestAPI.RequestAPIContext<K, CommandRequest<K, C>, CommandResponse> getRequestAPIContext(CommandSpec<K, C> commandSpec, KafkaConfig kafkaConfig) {
        ResourceNamingStrategy namingStrategy = commandSpec.resourceNamingStrategy();
        CommandSerdes<K, C> serdes = commandSpec.serdes();
        String responseTopicBase = namingStrategy.topicName(
                commandSpec.aggregateName(),
                command_response.name());

        HostInfo currentHost = kafkaConfig.currentHostInfo();
        String privateResponseTopic =  String.format("%s_%s_%d", responseTopicBase, currentHost.host(), currentHost.port());
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

