package io.simplesource.kafka.internal.streams.topologies;

import io.simplesource.data.Sequence;
import io.simplesource.kafka.internal.MockedInMemorySerde;
import io.simplesource.kafka.internal.streams.model.TestAggregate;
import io.simplesource.kafka.internal.util.Tuple;
import io.simplesource.kafka.model.AggregateUpdate;
import io.simplesource.kafka.model.AggregateUpdateResult;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.simplesource.kafka.api.AggregateResources.StateStoreEntity.command_response;
import static io.simplesource.kafka.internal.streams.topologies.EventSourcedTopologyTestUtility.aggregateUpdateResultWithSuccess;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CommandResultStoreUpdateConsumerTest {
    private static final String COMMAND_RESPONSE_STATE_STORE = "CommandResultStore";
    private static final String AGGREGATE_UPDATE_RESULT_INPUT_TOPIC = "AGGREGATE_UPDATE_RESULT_INPUT_TOPIC";
    private static final String AGGREGATE_KEY = "Key";
    private static final int WINDOW_SIZE = 30000;
    @Mock
    private AggregateStreamResourceNames aggregateStreamResourceNames;
    private Serde<UUID> keySerde = new MockedInMemorySerde<>();
    private Serde<AggregateUpdateResult<Optional<TestAggregate>>> streamValueSerde = new MockedInMemorySerde<>();

    private CommandResultStoreUpdateConsumer<String, Optional<TestAggregate>> target;
    private TimeWindows timeWindows = TimeWindows.of(WINDOW_SIZE).advanceBy(10000);

    private TopologyTestDriver topologyTestDriver;
    private ConsumerRecordFactory<String, AggregateUpdateResult<Optional<TestAggregate>>> consumerRecordFactory;

    @BeforeEach
    void setUp() {
        target = new CommandResultStoreUpdateConsumer<>(aggregateStreamResourceNames, timeWindows, keySerde, streamValueSerde);

        when(aggregateStreamResourceNames.stateStoreName(command_response)).thenReturn(COMMAND_RESPONSE_STATE_STORE);

        consumerRecordFactory = new ConsumerRecordFactory<>(AGGREGATE_UPDATE_RESULT_INPUT_TOPIC,
                Serdes.String().serializer(), new MockedInMemorySerde<AggregateUpdateResult<Optional<TestAggregate>>>().serializer());

        topologyTestDriver = new TopologyTestDriverInitializer()
                .withSourceTopicName(AGGREGATE_UPDATE_RESULT_INPUT_TOPIC)
                .<String, AggregateUpdateResult<Optional<TestAggregate>>>build(s -> target.accept(s));
    }

    @AfterEach
    void tearDown() {
        topologyTestDriver.close();
    }

    @Test
    void aggregateUpdateResultShouldBeRePartitionedAndStoredInWindowStore() {
        Sequence readSequence = Sequence.position(100);
        Sequence aggregateUpdateSequence = Sequence.position(200);
        List<AggregateUpdateResult<Optional<TestAggregate>>> aggregateUpdateResults = aggregateUpdateResults(3,
                readSequence, aggregateUpdateSequence);

        aggregateUpdateResults.forEach(a -> topologyTestDriver.pipeInput(consumerRecordFactory.create(AGGREGATE_UPDATE_RESULT_INPUT_TOPIC,
                AGGREGATE_KEY, a)));

        verifyWindowStoreContains(aggregateUpdateResults.stream().map(a -> Tuple.of(a.commandId(), a)).collect(Collectors.toList()));
    }

    @Test
    void shouldPunctuateIfWallClockTimeAdvances() {
        Sequence readSequence = Sequence.position(100);
        Sequence aggregateUpdateSequence = Sequence.position(200);
        List<AggregateUpdateResult<Optional<TestAggregate>>> aggregateUpdateResults = aggregateUpdateResults(5,
                readSequence, aggregateUpdateSequence);

        aggregateUpdateResults.forEach(a -> topologyTestDriver.pipeInput(consumerRecordFactory.create(AGGREGATE_UPDATE_RESULT_INPUT_TOPIC,
                AGGREGATE_KEY, a)));

        topologyTestDriver.advanceWallClockTime(WINDOW_SIZE);
        verifyEmptyWindowStore(System.currentTimeMillis());
    }

    private List<AggregateUpdateResult<Optional<TestAggregate>>> aggregateUpdateResults(int count, Sequence readSequence, Sequence aggregateUpdateSequence) {
        return createAggregateUpdatesWithLength(count, aggregateUpdateSequence)
                .map(a -> aggregateUpdateResultWithSuccess(readSequence, a))
                .collect(Collectors.toList());
    }

    private void verifyEmptyWindowStore(long windowStartTime) {
        assertThat(
                topologyTestDriver.getWindowStore(COMMAND_RESPONSE_STATE_STORE)
                        .fetchAll(windowStartTime, Long.MAX_VALUE))
                .hasSize(0);

    }

    private <V> void verifyWindowStoreContains(List<Tuple<UUID, V>> records) {
        long windowStartTime = System.currentTimeMillis();
        verifyWindowStoreContains(records, windowStartTime, windowStartTime+WINDOW_SIZE);
    }

    private <V> void verifyWindowStoreContains(List<Tuple<UUID, V>> records, long windowStartTime, long windowEndTime) {
        WindowStore<UUID, V> windowStore = topologyTestDriver.getWindowStore(COMMAND_RESPONSE_STATE_STORE);

        records.forEach(r -> windowStore.fetch(r.v1(), windowStartTime, windowEndTime)
                .forEachRemaining(kv -> assertThat(kv.value).isEqualTo(r.v2())));
    }

    private static Stream<AggregateUpdate<Optional<TestAggregate>>> createAggregateUpdatesWithLength(int count, Sequence startSequence) {
        return IntStream.range(0, count)
                .mapToObj(i -> new AggregateUpdate<>(Optional.of(new TestAggregate("Aggregate Name " + i)), Sequence.position(startSequence.getSeq() + i)));
    }
}