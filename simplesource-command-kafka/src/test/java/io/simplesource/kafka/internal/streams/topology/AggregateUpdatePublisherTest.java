package io.simplesource.kafka.internal.streams.topology;

import io.simplesource.api.CommandError;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.data.Sequence;
import io.simplesource.kafka.api.AggregateResources.StateStoreEntity;
import io.simplesource.kafka.api.AggregateResources.TopicEntity;
import io.simplesource.kafka.internal.streams.MockInMemorySerde;
import io.simplesource.kafka.internal.streams.model.TestAggregate;
import io.simplesource.kafka.internal.streams.model.TestCommand;
import io.simplesource.kafka.internal.streams.model.TestEvent;
import io.simplesource.kafka.internal.util.Tuple;
import io.simplesource.kafka.model.AggregateUpdate;
import io.simplesource.kafka.model.AggregateUpdateResult;
import io.simplesource.kafka.model.CommandResponse;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.simplesource.api.CommandError.Reason.UnexpectedErrorCode;
import static org.assertj.core.api.Assertions.assertThat;
import static io.simplesource.kafka.internal.streams.topology.EventSourcedTopologyTestUtility.*;


@ExtendWith(MockitoExtension.class)
class AggregateUpdatePublisherTest {
    private static final String AGGREGATE_UPDATE_RESULT_INPUT_TOPIC = "AGGREGATE_UPDATE_RESULT_INPUT_TOPIC";
    private static final String AGGREGATE_KEY = "key";
    private static final int WINDOW_SIZE = 30000;

    private Serde<String> aggregateKeySerde = Serdes.String();
    private Serde<AggregateUpdateResult<Optional<TestAggregate>>> aggregateUpdateResultValueSerde = new MockInMemorySerde<>();
    private Serde<CommandResponse> commandResponseValueSerde = new MockInMemorySerde<>();
    private TopologyTestDriver topologyTestDriver;
    private TopologyTestDriver topologyTestDriverIn;
    private ConsumerRecordFactory<String, AggregateUpdateResult<Optional<TestAggregate>>> consumerRecordFactory;

    private String consumerResponseTopic = TestContextBuilder.topicName(TopicEntity.command_response);

    @BeforeEach
    void setUp() {
        TopologyContext<String, TestCommand, TestEvent, Optional<TestAggregate>> context =
                new TestContextBuilder()
                        .buildContext();

        topologyTestDriver = new TopologyTestDriverInitializer()
                .withStateStore(context.stateStoreName(StateStoreEntity.aggregate_update), aggregateKeySerde, aggregateUpdateResultValueSerde)
                .build(builder -> {

                });

        consumerRecordFactory = new ConsumerRecordFactory<>(aggregateKeySerde.serializer(), aggregateUpdateResultValueSerde.serializer());
    }

    TopologyContext<String, TestCommand, TestEvent, Optional<TestAggregate>> context;

    @AfterEach
    void tearDown() {
        topologyTestDriver.close();
    }

    @Test
    void shouldUpdateAggregateStateStoreWhenAggregateUpdateResultIsSuccessfull() {
        Sequence readSequence = Sequence.position(100);
        Sequence aggregateUpdateSequence = Sequence.position(200);
        AggregateUpdate<Optional<TestAggregate>> aggregateUpdate = new AggregateUpdate<>(Optional.empty(), aggregateUpdateSequence);

        topologyTestDriver.pipeInput(consumerRecordFactory.create(AGGREGATE_UPDATE_RESULT_INPUT_TOPIC,
                AGGREGATE_KEY, EventSourcedTopologyTestUtility.aggregateUpdateResultWithSuccess(readSequence, aggregateUpdate)));

        assertThat(keyValueStore(StateStoreEntity.aggregate_update).get(AGGREGATE_KEY)).isEqualTo(aggregateUpdate);
    }

    @Test
    void shouldNotUpdateAggregateStateStoreWhenAggregateUpdateResultIsFailure() {
        Sequence readSequence = Sequence.position(100);

        topologyTestDriver.pipeInput(consumerRecordFactory.create(AGGREGATE_UPDATE_RESULT_INPUT_TOPIC,
                AGGREGATE_KEY, aggregateUpdateResultWithFailure(readSequence, NonEmptyList.of(CommandError.Reason.UnexpectedErrorCode))));

        assertThat(keyValueStore(StateStoreEntity.aggregate_update).get(AGGREGATE_KEY)).isNull();
    }

    @Test
    void aggregateUpdateResultShouldBeRePartitionedAndStoredInCommandResponseWindowStore() {
        Sequence readSequence = Sequence.position(100);
        Sequence aggregateUpdateSequence = Sequence.position(200);
        List<AggregateUpdateResult<Optional<TestAggregate>>> aggregateUpdateResults = aggregateUpdateResults(3,
                readSequence, aggregateUpdateSequence);

        aggregateUpdateResults.forEach(a -> topologyTestDriver.pipeInput(consumerRecordFactory.create(AGGREGATE_UPDATE_RESULT_INPUT_TOPIC,
                AGGREGATE_KEY, a)));

        verifyWindowStoreContains(aggregateUpdateResults.stream().map(a -> Tuple.of(a.commandId(), a)).collect(Collectors.toList()),
                StateStoreEntity.command_response);
    }

    @Test
    void commandResponseStoreShouldPunctuateIfWallClockTimeAdvances() {
        Sequence readSequence = Sequence.position(100);
        Sequence aggregateUpdateSequence = Sequence.position(200);
        List<AggregateUpdateResult<Optional<TestAggregate>>> aggregateUpdateResults = aggregateUpdateResults(5,
                readSequence, aggregateUpdateSequence);

        aggregateUpdateResults.forEach(a -> topologyTestDriver.pipeInput(consumerRecordFactory.create(AGGREGATE_UPDATE_RESULT_INPUT_TOPIC,
                AGGREGATE_KEY, a)));

        topologyTestDriver.advanceWallClockTime(WINDOW_SIZE);
        verifyEmptyWindowStore(System.currentTimeMillis(), StateStoreEntity.command_response);
    }

    @Test
    void successAggregateUpdateResultShouldBeMappedAndPassedThroughToCommandResponseTopic() {
        Sequence readSequence = Sequence.position(100);
        Sequence aggregateUpdateSequence = Sequence.position(200);
        AggregateUpdate<Optional<TestAggregate>> aggregateUpdate = new AggregateUpdate<>(Optional.empty(), aggregateUpdateSequence);
        AggregateUpdateResult<Optional<TestAggregate>> aggregateUpdateResult = aggregateUpdateResultWithSuccess(readSequence, aggregateUpdate);

        topologyTestDriver.pipeInput(consumerRecordFactory.create(AGGREGATE_UPDATE_RESULT_INPUT_TOPIC,
                AGGREGATE_KEY, aggregateUpdateResult));

        ProducerRecord<String, CommandResponse> record = topologyTestDriver.readOutput(
                consumerResponseTopic, aggregateKeySerde.deserializer(),
                commandResponseValueSerde.deserializer());
        OutputVerifier.compareKeyValue(record, AGGREGATE_KEY, new CommandResponse(aggregateUpdateResult.commandId(),
                readSequence, Result.success(aggregateUpdateSequence)));
    }

    @Test
    void failureAggregateUpdateResultShouldBeMappedAndPassedThroughToCommandResponseTopic() {
        Sequence readSequence = Sequence.position(100);
        NonEmptyList<CommandError> commandErrors = NonEmptyList.of(CommandError.of(UnexpectedErrorCode, "Error message"),
                CommandError.of(CommandError.Reason.InvalidReadSequence, "Error message 2"));
        UUID commandId = UUID.randomUUID();
        AggregateUpdateResult<Optional<TestAggregate>> aggregateUpdateResult =
                new AggregateUpdateResult<>(commandId, readSequence, Result.failure(commandErrors));

        topologyTestDriver.pipeInput(consumerRecordFactory.create(AGGREGATE_UPDATE_RESULT_INPUT_TOPIC,
                AGGREGATE_KEY, aggregateUpdateResult));

        ProducerRecord<String, CommandResponse> record = topologyTestDriver.readOutput(
                consumerResponseTopic, aggregateKeySerde.deserializer(), commandResponseValueSerde.deserializer());
        OutputVerifier.compareKeyValue(record, AGGREGATE_KEY, new CommandResponse(commandId, readSequence, Result.failure(commandErrors)));
    }

    private List<AggregateUpdateResult<Optional<TestAggregate>>> aggregateUpdateResults(int count, Sequence readSequence, Sequence aggregateUpdateSequence) {
        return createAggregateUpdatesWithLength(count, aggregateUpdateSequence)
                .map(a -> aggregateUpdateResultWithSuccess(readSequence, a))
                .collect(Collectors.toList());
    }

    private void verifyEmptyWindowStore(long windowStartTime, StateStoreEntity stateStoreEntity) {
        assertThat(
                topologyTestDriver.getWindowStore(TestContextBuilder.stateStoreName(stateStoreEntity))
                        .fetchAll(windowStartTime, Long.MAX_VALUE))
                .hasSize(0);

    }

    private <V> void verifyWindowStoreContains(List<Tuple<UUID, V>> records, StateStoreEntity stateStoreEntity) {
        long windowStartTime = System.currentTimeMillis();
        verifyWindowStoreContains(records, windowStartTime, windowStartTime+WINDOW_SIZE, stateStoreEntity);
    }

    private <V> void verifyWindowStoreContains(List<Tuple<UUID, V>> records, long windowStartTime, long windowEndTime,
                                               StateStoreEntity stateStoreEntity) {
        WindowStore<UUID, V> windowStore = topologyTestDriver
                .getWindowStore(TestContextBuilder.stateStoreName(stateStoreEntity));

        records.forEach(r -> windowStore.fetch(r.v1(), windowStartTime, windowEndTime)
                .forEachRemaining(kv -> assertThat(kv.value).isEqualTo(r.v2())));
    }

    private static Stream<AggregateUpdate<Optional<TestAggregate>>> createAggregateUpdatesWithLength(int count, Sequence startSequence) {
        return IntStream.range(0, count)
                .mapToObj(i -> new AggregateUpdate<>(Optional.of(new TestAggregate("Aggregate Name " + i)), Sequence.position(startSequence.getSeq() + i)));
    }

    @SuppressWarnings("unchecked")
    private KeyValueStore<String, AggregateUpdate<Optional<TestAggregate>>> keyValueStore(StateStoreEntity stateStoreEntity) {
        return (KeyValueStore<String, AggregateUpdate<Optional<TestAggregate>>>)
                topologyTestDriver.getStateStore(TestContextBuilder.stateStoreName(stateStoreEntity));
    }
}