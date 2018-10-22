package io.simplesource.kafka.internal.streams.topologies;

import io.simplesource.api.CommandError;
import io.simplesource.api.CommandHandler;
import io.simplesource.api.InitialValue;
import io.simplesource.api.InvalidSequenceHandler;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.data.Sequence;
import io.simplesource.kafka.internal.MockedInMemorySerde;
import io.simplesource.kafka.internal.streams.model.TestAggregate;
import io.simplesource.kafka.internal.streams.model.TestCommand;
import io.simplesource.kafka.internal.streams.model.TestEvent;
import io.simplesource.kafka.model.AggregateUpdate;
import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.kafka.model.ValueWithSequence;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Optional;
import java.util.UUID;

import static io.simplesource.kafka.api.AggregateResources.StateStoreEntity.aggregate_update;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("unchecked")
class CommandRequestTransformerTest {
    private static final String AGGREGATE_UPDATE_STORE_NAME = "AggregateUpdateStoreName";
    private static final String NAME = "Name1";
    private static final String AGGREGATE_KEY = "key";

    @Mock
    private InitialValue<String, Optional<TestAggregate>> initialValue;
    @Mock
    private InvalidSequenceHandler<String, TestCommand, Optional<TestAggregate>> invalidSequenceHandler;
    @Mock
    private CommandHandler<String, TestCommand, TestEvent, Optional<TestAggregate>> commandHandler;
    @Mock
    private AggregateStreamResourceNames aggregateResourceNames;
    private KeyValueStore<String, AggregateUpdate<Optional<TestAggregate>>> stateStore;
    private ProcessorContext processorContext;

    private CommandRequestTransformer<String, TestCommand, TestEvent, Optional<TestAggregate>> target;
    private Sequence claimedAggregateSequence = Sequence.position(200);
    private Sequence actualAggregateSequence = Sequence.position(100);
    private Optional<TestAggregate> currentAggregate = Optional.empty();

    @BeforeEach
    void setUp() {
        target = new CommandRequestTransformer<>(initialValue, invalidSequenceHandler, commandHandler,
                aggregateResourceNames);

        when(aggregateResourceNames.stateStoreName(aggregate_update)).thenReturn(AGGREGATE_UPDATE_STORE_NAME);
        processorContext = new MockProcessorContext();
        setupStateStore();

        target.init(processorContext);
    }

    @AfterEach
    void tearDown() throws IOException {
        stateStore.close();
        //RocksDBStore does not expose the file path or even ability to clean up the store
        Files.walk(Paths.get("rocksdb", AGGREGATE_UPDATE_STORE_NAME))
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }

    @Test
    void initShouldRetrieveStateStoreOfAggregateUpdate() {
        assertThat(processorContext.getStateStore(AGGREGATE_UPDATE_STORE_NAME)).isEqualTo(stateStore);
    }

    @Test
    void transformShouldCallInvalidSequenceHandlerWhenCommandRequestSequenceIsNotTheSameAsCurrentAggregateSequence() {
        CommandError commandError = CommandError.of(CommandError.Reason.InvalidReadSequence, "Error message");
        TestCommand command = new TestCommand.CreateCommand(NAME);
        when(invalidSequenceHandler.shouldReject(AGGREGATE_KEY, actualAggregateSequence, claimedAggregateSequence,
                currentAggregate, command)).thenReturn(Optional.of(commandError));
        stateStore.put(AGGREGATE_KEY, new AggregateUpdate<>(currentAggregate, actualAggregateSequence));

        CommandEvents<TestEvent, Optional<TestAggregate>> actualResult = target.transform(AGGREGATE_KEY,
                new CommandRequest<>(command, claimedAggregateSequence, UUID.randomUUID()));

        assertThat(actualResult.eventValue().failureReasons()).contains(NonEmptyList.of(commandError));
    }

    @Test
    void givenStateStoreDoesNotHaveAggregateKeyTransformShouldUseInitialValueForAggregate() {
        TestEvent event = new TestEvent.Created(NAME);
        TestCommand command = new TestCommand.CreateCommand(NAME);
        ValueWithSequence<TestEvent> valueWithSequence = new ValueWithSequence<>(event, Sequence.position(1));

        when(initialValue.empty(AGGREGATE_KEY)).thenReturn(currentAggregate);
        configureCommandHandlerWithResultEvents(currentAggregate, command, event);

        CommandEvents<TestEvent, Optional<TestAggregate>> actualResult = target.transform(AGGREGATE_KEY,
                new CommandRequest<>(command, claimedAggregateSequence, UUID.randomUUID()));

        assertThat(actualResult.eventValue().getOrElse(null)).containsExactly(valueWithSequence);
    }

    @Test
    void givenSequencesAreMatchedTransformShouldProcessCommandAndReturnResultEvents() {
        TestCommand command = new TestCommand.CreateCommand(NAME);
        AggregateUpdate<Optional<TestAggregate>> aggregateUpdate = new AggregateUpdate<>(currentAggregate, actualAggregateSequence);
        TestEvent event = new TestEvent.Created(NAME);
        ValueWithSequence<TestEvent> valueWithSequence = new ValueWithSequence<>(event, actualAggregateSequence.next());
        stateStore.put(AGGREGATE_KEY, aggregateUpdate);
        configureCommandHandlerWithResultEvents(currentAggregate, command, event);

        CommandEvents<TestEvent, Optional<TestAggregate>> actualResult = target.transform(AGGREGATE_KEY,
                new CommandRequest<>(command, actualAggregateSequence, UUID.randomUUID()));

        assertThat(actualResult.eventValue().getOrElse(null)).containsExactly(valueWithSequence);
    }

    @Test
    void transformShouldUseInitialValueForAggregateWhenRetrieveAggregateUpdateFromStateStoreThrowsException() {
        TestEvent event = new TestEvent.Created(NAME);
        TestCommand command = new TestCommand.CreateCommand(NAME);
        ValueWithSequence<TestEvent> valueWithSequence = new ValueWithSequence<>(event, Sequence.position(1));
        Optional<TestAggregate> initialAggregateUpdate = Optional.of(new TestAggregate(NAME));

        stateStore.close();
        when(initialValue.empty(AGGREGATE_KEY)).thenReturn(initialAggregateUpdate);
        configureCommandHandlerWithResultEvents(initialAggregateUpdate, command, event);

        CommandEvents<TestEvent, Optional<TestAggregate>> actualResult = target.transform(AGGREGATE_KEY,
                new CommandRequest<>(command, claimedAggregateSequence, UUID.randomUUID()));

        assertThat(actualResult.eventValue().getOrElse(null)).containsExactly(valueWithSequence);
    }

    private void configureCommandHandlerWithResultEvents(Optional<TestAggregate> aggregate, TestCommand command, TestEvent event, TestEvent... events) {
        when(commandHandler.interpretCommand(AGGREGATE_KEY, aggregate, command)).thenReturn(Result.success(NonEmptyList.of(event, events)));
    }

    private void setupStateStore() {
        stateStore =
                Stores.keyValueStoreBuilder(
                        //We are using RocksDb instead of in-memory to simulate the InvalidStateStore case
                        Stores.persistentKeyValueStore(AGGREGATE_UPDATE_STORE_NAME),
                        Serdes.String(),
                        new MockedInMemorySerde<AggregateUpdate<Optional<TestAggregate>>>())
                        .withLoggingDisabled()
                        .build();

        stateStore.init(processorContext, stateStore);
        processorContext.register(stateStore, null);
    }
}