package io.simplesource.kafka.internal.streams.topologies;

import io.simplesource.api.CommandError;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Sequence;
import io.simplesource.kafka.internal.MockedInMemorySerde;
import io.simplesource.kafka.internal.streams.model.TestAggregate;
import io.simplesource.kafka.model.AggregateUpdate;
import io.simplesource.kafka.model.AggregateUpdateResult;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static io.simplesource.kafka.api.AggregateResources.StateStoreEntity.aggregate_update;
import static io.simplesource.kafka.internal.streams.topologies.EventSourcedTopologyTestUtility.aggregateUpdateResultWithFailure;
import static io.simplesource.kafka.internal.streams.topologies.EventSourcedTopologyTestUtility.aggregateUpdateResultWithSuccess;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AggregateStoreUpdateConsumerTest {
    private static final String AGGREGATE_UPDATE_STATE_STORE = "AggregateUpdateStore";
    private static final String AGGREGATE_UPDATE_RESULT_INPUT_TOPIC = "AGGREGATE_UPDATE_RESULT_INPUT_TOPIC";
    private static final String AGGREGATE_KEY = "key";

    @Mock
    private AggregateStreamResourceNames aggregateStreamResourceNames;
    private Serde<String> keySerde = Serdes.String();
    private Serde<AggregateUpdateResult<Optional<TestAggregate>>> valueSerde = new MockedInMemorySerde<>();
    private TopologyTestDriver topologyTestDriver;
    private ConsumerRecordFactory<String, AggregateUpdateResult<Optional<TestAggregate>>> consumerRecordFactory;

    private AggregateStoreUpdateConsumer<String, Optional<TestAggregate>> target;

    @BeforeEach
    void setUp() {
        target = new AggregateStoreUpdateConsumer<>(aggregateStreamResourceNames);

        when(aggregateStreamResourceNames.stateStoreName(aggregate_update)).thenReturn(AGGREGATE_UPDATE_STATE_STORE);

        topologyTestDriver = new TopologyTestDriverInitializer()
                .withStateStore(AGGREGATE_UPDATE_STATE_STORE, keySerde, valueSerde)
                .withSourceTopicName(AGGREGATE_UPDATE_RESULT_INPUT_TOPIC)
                .<String, AggregateUpdateResult<Optional<TestAggregate>>>build(s -> target.accept(s));
        consumerRecordFactory = new ConsumerRecordFactory<>(keySerde.serializer(), valueSerde.serializer());
    }

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
                AGGREGATE_KEY, aggregateUpdateResultWithSuccess(readSequence, aggregateUpdate)));

        assertThat(stateStore().get(AGGREGATE_KEY)).isEqualTo(aggregateUpdate);
    }

    @Test
    void shouldNotUpdateAggregateStateStoreWhenAggregateUpdateResultIsFailure() {
        Sequence readSequence = Sequence.position(100);

        topologyTestDriver.pipeInput(consumerRecordFactory.create(AGGREGATE_UPDATE_RESULT_INPUT_TOPIC,
                AGGREGATE_KEY, aggregateUpdateResultWithFailure(readSequence, NonEmptyList.of(CommandError.Reason.UnexpectedErrorCode))));

        assertThat(stateStore().get(AGGREGATE_KEY)).isNull();
    }

    @SuppressWarnings("unchecked")
    private KeyValueStore<String, AggregateUpdate<Optional<TestAggregate>>>  stateStore() {
        return (KeyValueStore<String, AggregateUpdate<Optional<TestAggregate>>>) topologyTestDriver.getStateStore(AGGREGATE_UPDATE_STATE_STORE);
    }
}