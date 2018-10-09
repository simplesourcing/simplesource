package io.simplesource.kafka.internal.streams;

import io.simplesource.api.Aggregator;
import io.simplesource.api.InitialValue;
import io.simplesource.api.CommandError;
import io.simplesource.api.CommandError.Reason;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.data.Sequence;
import io.simplesource.kafka.api.AggregateResources;
import io.simplesource.kafka.api.AggregateSerdes;
import io.simplesource.kafka.model.*;
import io.simplesource.kafka.spec.AggregateSpec;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static io.simplesource.data.Result.failure;
import static io.simplesource.kafka.api.AggregateResources.StateStoreEntity.aggregate_update;
import static io.simplesource.kafka.api.AggregateResources.StateStoreEntity.command_response;
import static io.simplesource.kafka.api.AggregateResources.TopicEntity.*;
import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;

/**
 * @param <A> the aggregate aggregate_update
 * @param <E> all events generated for this aggregate
 * @param <C> all commands for this aggregate
 * @param <K> the aggregate key
 */
public final class EventSourcedTopology<K, C, E, A> {
    private static final Logger logger = LoggerFactory.getLogger(EventSourcedTopology.class);

    private final AggregateSpec<K, C, E, A> aggregateSpec;
    private final long commandResponseRetentionInSeconds;
    private final AggregateSerdes<K, C, E, A> serdes;
    private final Aggregator<E, A> aggregator;
    private final InitialValue<K, A> initialValue;

    private final Consumed<K, CommandRequest<C>> commandEventsConsumed;
    private final Produced<K, ValueWithSequence<E>> eventsConsumedProduced;
    private final Produced<K, AggregateUpdate<A>> aggregatedUpdateProduced;
    private final Serialized<UUID, AggregateUpdateResult<A>> serializedAggregateUpdate;

    public EventSourcedTopology(
            final AggregateSpec<K, C, E, A> aggregateSpec
    ) {
        this.aggregateSpec = aggregateSpec;
        this.commandResponseRetentionInSeconds = aggregateSpec.generation().stateStoreSpec().retentionInSeconds();
        serdes = aggregateSpec.serialization().serdes();
        aggregator = aggregateSpec.generation().aggregator();
        initialValue = aggregateSpec.generation().initialValue();

        commandEventsConsumed = Consumed.with(serdes.aggregateKey(), serdes.commandRequest());
        eventsConsumedProduced = Produced.with(serdes.aggregateKey(), serdes.valueWithSequence());
        aggregatedUpdateProduced = Produced.with(serdes.aggregateKey(), serdes.aggregateUpdate());
        serializedAggregateUpdate = Serialized.with(serdes.commandResponseKey(), serdes.updateResult());
    }

    public void addTopology(final StreamsBuilder builder) {
        addStateStores(builder);

        final KStream<K, CommandEvents<E, A>> eventResultStream = eventResultStream(builder);
        publishEvents(eventResultStream);

        final KStream<K, AggregateUpdateResult<A>> aggregateUpdateStream = aggregateUpdateStream(eventResultStream);
        publishAggregateUpdates(aggregateUpdateStream);

        updateAggregateStateStore(aggregateUpdateStream);
        updateCommandResultStore(aggregateUpdateStream);
    }

    private void addStateStores(final StreamsBuilder builder) {
        final KeyValueStoreBuilder<K, AggregateUpdate<A>> aggregateStoreBuilder = new KeyValueStoreBuilder<>(
                persistentKeyValueStore(storeName(aggregate_update)),
                serdes.aggregateKey(),
                serdes.aggregateUpdate(),
                Time.SYSTEM);
        builder.addStateStore(aggregateStoreBuilder);
    }

    private KStream<K, CommandEvents<E, A>> eventResultStream(final StreamsBuilder builder) {
        final KStream<K, CommandRequest<C>> requestStream = builder.stream(
                topicName(command_request), commandEventsConsumed);
        return requestStream
                .transformValues(CommandRequestTransformer::new, storeName(aggregate_update));
    }

    private void publishEvents(final KStream<K, CommandEvents<E, A>> eventResultStream) {
        final KStream<K, ValueWithSequence<E>> eventStream = eventResultStream.flatMapValues(result -> result.eventValue()
                .fold(
                        reasons -> Collections.emptyList(),
                        eventsWithSequence -> eventsWithSequence
                                .stream()
                                .collect(Collectors.toList())));
        eventStream
                .peek((k, v) -> logger.debug("Writing event ({},{}) to {}", k, v, topicName(event)))
                .to(topicName(event), eventsConsumedProduced);
    }

    private KStream<K, AggregateUpdateResult<A>> aggregateUpdateStream(final KStream<K, CommandEvents<E, A>> eventResultStream) {
        return eventResultStream
                .mapValues((serializedKey, result) -> {
                    final Result<CommandError, AggregateUpdate<A>> aggregateUpdateResult = result.eventValue().map(events -> {
                        final BiFunction<AggregateUpdate<A>, ValueWithSequence<E>, AggregateUpdate<A>> reducer =
                                (aggregateUpdate, eventWithSequence) -> new AggregateUpdate<>(
                                        aggregator.applyEvent(aggregateUpdate.aggregate(), eventWithSequence.value()),
                                        eventWithSequence.sequence()
                                );
                        return events.fold(
                                eventWithSequence -> new AggregateUpdate<>(
                                        aggregator.applyEvent(result.aggregate(), eventWithSequence.value()),
                                        eventWithSequence.sequence()
                                ),
                                reducer
                        );
                    });
                    return new AggregateUpdateResult<>(
                            result.commandId(),
                            result.readSequence(),
                            aggregateUpdateResult);
                });
    }

    private void publishAggregateUpdates(final KStream<K, AggregateUpdateResult<A>> aggregateUpdateStream) {
        final KStream<K, AggregateUpdate<A>> aggregateStream = aggregateUpdateStream
                .flatMapValues(update -> update.updatedAggregateResult().fold(
                        reasons -> Collections.emptyList(),
                        aggregateUpdate -> Collections.singletonList(aggregateUpdate)
                ));
        aggregateStream.to(topicName(aggregate), aggregatedUpdateProduced);
    }

    /**
     * Update the state store with the latest aggregate_update value on successful updates.
     */
    private void updateAggregateStateStore(final KStream<K, AggregateUpdateResult<A>> aggregateUpdateStream) {
        aggregateUpdateStream.process(() -> new Processor<K, AggregateUpdateResult<A>>() {
            private KeyValueStore<K, AggregateUpdate<A>> stateStore;

            @Override
            public void init(final ProcessorContext context) {
                stateStore = (KeyValueStore<K, AggregateUpdate<A>>) context.getStateStore(storeName(aggregate_update));
            }

            @Override
            public void process(final K readOnlyKey, final AggregateUpdateResult<A> aggregateUpdateResult) {
                aggregateUpdateResult.updatedAggregateResult().ifSuccessful(
                        aggregateUpdate -> stateStore.put(
                                readOnlyKey,
                                aggregateUpdate));
            }

            @Override
            public void close() {
            }
        }, storeName(aggregate_update));
    }

    private void updateCommandResultStore(final KStream<K, AggregateUpdateResult<A>> aggregateUpdateStream) {

        final long retentionMillis = TimeUnit.SECONDS.toMillis(commandResponseRetentionInSeconds);
        aggregateUpdateStream
                .map((k, v) -> KeyValue.pair(
                        v.commandId(),
                        v))
                .groupByKey(serializedAggregateUpdate)
                .windowedBy(
                        TimeWindows
                                .of(retentionMillis)
                                .advanceBy(retentionMillis / 3L))
                .reduce((current, latest) -> latest, materializedWindow(storeName(command_response)));
    }

    private Materialized<UUID, AggregateUpdateResult<A>, WindowStore<Bytes, byte[]>> materializedWindow(final String storeName) {
        return Materialized
                .<UUID, AggregateUpdateResult<A>, WindowStore<Bytes, byte[]>>as(storeName)
                .withKeySerde(serdes.commandResponseKey())
                .withValueSerde(serdes.updateResult());
    }

    private final class CommandRequestTransformer implements ValueTransformerWithKey<K, CommandRequest<C>, CommandEvents<E, A>> {
        private ReadOnlyKeyValueStore<K, AggregateUpdate<A>> stateStore;

        @Override
        public void init(final ProcessorContext context) {
            stateStore = (ReadOnlyKeyValueStore<K, AggregateUpdate<A>>) context.getStateStore(storeName(aggregate_update));
        }

        @Override
        public CommandEvents<E, A> transform(final K readOnlyKey, final CommandRequest<C> request) {

            AggregateUpdate<A> currentUpdatePre;
            try {
                currentUpdatePre = Optional.ofNullable(stateStore.get(readOnlyKey))
                        .orElse(AggregateUpdate.of(initialValue.empty(readOnlyKey)));
            } catch (Exception e) {
                currentUpdatePre = AggregateUpdate.of(initialValue.empty(readOnlyKey));
            }
            final AggregateUpdate<A> currentUpdate = currentUpdatePre;

            Result<CommandError, NonEmptyList<E>> commandResult;
            try {
                Optional<CommandError> maybeReject =
                        Objects.equals(request.readSequence(), currentUpdate.sequence()) ? Optional.empty() :
                            aggregateSpec.generation().invalidSequenceHandler().shouldReject(
                                readOnlyKey,
                                currentUpdate.sequence(),
                                request.readSequence(),
                                currentUpdate.aggregate(),
                                request.command());

                commandResult = maybeReject.<Result<CommandError, NonEmptyList<E>>>map(
                        commandErrorReason -> Result.failure(commandErrorReason)).orElseGet(
                                () -> aggregateSpec.generation().commandHandler().interpretCommand(
                                        readOnlyKey,
                                        currentUpdate.aggregate(),
                                        request.command()));
            } catch (final Exception e) {
                logger.warn("[{} aggregate] Failed to apply command handler on key {} to request {}",
                        aggregateSpec.aggregateName(), readOnlyKey, request, e);
                commandResult = failure(CommandError.of(Reason.CommandHandlerFailed, e));
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

        @Override
        public void close() {
        }

    }

    private String topicName(final AggregateResources.TopicEntity topic) {
        return aggregateSpec
                .serialization()
                .resourceNamingStrategy()
                .topicName(aggregateSpec.aggregateName(), topic.name());
    }

    private String storeName(final AggregateResources.StateStoreEntity store) {
        return aggregateSpec
                .serialization()
                .resourceNamingStrategy()
                .storeName(aggregateSpec.aggregateName(), store.name());
    }
}
