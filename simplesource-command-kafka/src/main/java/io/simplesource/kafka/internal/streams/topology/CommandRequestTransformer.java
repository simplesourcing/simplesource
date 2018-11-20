package io.simplesource.kafka.internal.streams.topology;

import io.simplesource.api.CommandError;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.data.Sequence;
import io.simplesource.kafka.model.AggregateUpdate;
import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.kafka.model.ValueWithSequence;
import io.simplesource.kafka.spec.AggregateSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;

import static io.simplesource.data.Result.failure;

final class CommandRequestTransformer {
    private static final Logger logger = LoggerFactory.getLogger(CommandRequestTransformer.class);
    static <K, C, E, A> CommandEvents<E, A> getCommandEvents(
            TopologyContext<K, C, E, A> ctx,
            final AggregateUpdate<A> currentUpdateInput, final CommandRequest<K, C> request) {

        final K readOnlyKey = request.aggregateKey();
        AggregateSpec.Generation<K, C, E, A> generation = ctx.aggregateSpec().generation();

        AggregateUpdate<A> currentUpdatePre;
        try {
            currentUpdatePre = Optional.ofNullable(currentUpdateInput)
                    .orElse(AggregateUpdate.of(ctx.initialValue().empty(readOnlyKey)));
        } catch (Exception e) {
            currentUpdatePre = AggregateUpdate.of(ctx.initialValue().empty(readOnlyKey));
        }
        final AggregateUpdate<A> currentUpdate = currentUpdatePre;

        Result<CommandError, NonEmptyList<E>> commandResult;
        try {
            Optional<CommandError> maybeReject =
                    Objects.equals(request.readSequence(), currentUpdate.sequence()) ? Optional.empty() :
                            generation.invalidSequenceHandler().shouldReject(
                                    readOnlyKey,
                                    currentUpdate.sequence(),
                                    request.readSequence(),
                                    currentUpdate.aggregate(),
                                    request.command());

            commandResult = maybeReject.<Result<CommandError, NonEmptyList<E>>>map(
                    commandErrorReason -> Result.failure(commandErrorReason)).orElseGet(
                    () -> generation.commandHandler().interpretCommand(
                            readOnlyKey,
                            currentUpdate.aggregate(),
                            request.command()));
        } catch (final Exception e) {
            logger.warn("[{} aggregate] Failed to apply command handler on key {} to request {}",
                    ctx.aggregateSpec().aggregateName(), readOnlyKey, request, e);
            commandResult = failure(CommandError.of(CommandError.Reason.CommandHandlerFailed, e));
        }
        final Result<CommandError, NonEmptyList<ValueWithSequence<E>>> eventsResult = commandResult.map(
                eventList -> {
                    // get round Java limitation of only using finals in lambdas by wrapping in an array
                    final Sequence[] eventSequence = {currentUpdate.sequence()};
                    return eventList.map(event -> {
                        eventSequence[0] = eventSequence[0].next();
                        return new ValueWithSequence<>(event, eventSequence[0]);
                    });
                });
        return new CommandEvents<>(
                request.commandId(),
                request.readSequence(),
                currentUpdate.aggregate(),
                eventsResult
        );
    }
}
