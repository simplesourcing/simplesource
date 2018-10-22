package io.simplesource.kafka.internal.streams.topologies;

import io.simplesource.api.Aggregator;
import io.simplesource.api.CommandError;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Sequence;
import io.simplesource.kafka.api.AggregateResources;
import io.simplesource.kafka.internal.MockedInMemorySerde;
import io.simplesource.kafka.internal.streams.model.TestAggregate;
import io.simplesource.kafka.internal.streams.model.TestEvent;
import io.simplesource.kafka.model.AggregateUpdate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Optional;

import static io.simplesource.kafka.internal.streams.topologies.EventSourcedTopologyTestUtility.buildFailureCommandEvents;
import static io.simplesource.kafka.internal.streams.topologies.EventSourcedTopologyTestUtility.buildSuccessCommandEvents;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class EventProcessingSubTopologyTest {
    private static final String EVENTS_TOPIC = "events_topic";
    private static final String AGGREGATE_UPDATE_RESULT_TOPIC = "aggregateUpdateResult";

    private TestEvent.Created event1 = new TestEvent.Created("Event1");
    private TestEvent.Created event2 = new TestEvent.Created("Event2");
    private String aggregateKey = "key";
    private Sequence readSequence = Sequence.position(100);

    @Mock
    private AggregateStreamResourceNames aggregateResourceNames;

    private ConsumerRecordFactory<String, CommandEvents<TestEvent, Optional<TestAggregate>>> consumerRecordFactory;
    private TopologyTestDriver topologyTestDriver;
    private Serde<String> keySerde = Serdes.String();
    private Serde<AggregateUpdate<Optional<TestAggregate>>> aggregateUpdateSerde = new MockedInMemorySerde<>();

    private EventProcessingSubTopology<String, TestEvent, Optional<TestAggregate>> target;


    @BeforeEach
    void setUp() {
        target = new EventProcessingSubTopology<>(aggregateResourceNames, accumulatedEventNamesAggregator(), keySerde,
                aggregateUpdateSerde);

        when(aggregateResourceNames.topicName(AggregateResources.TopicEntity.event)).thenReturn(EVENTS_TOPIC);
        when(aggregateResourceNames.topicName(AggregateResources.TopicEntity.aggregate)).thenReturn(AGGREGATE_UPDATE_RESULT_TOPIC);

        topologyTestDriver = new TopologyTestDriverInitializer()
                .withSourceTopicName(EVENTS_TOPIC)
                .<String, CommandEvents<TestEvent, Optional<TestAggregate>>>build(s -> target.add(s));

        consumerRecordFactory = new ConsumerRecordFactory<>(keySerde.serializer(),
                new MockedInMemorySerde<CommandEvents<TestEvent, Optional<TestAggregate>>>().serializer());
    }

    @AfterEach
    void tearDown() {
        topologyTestDriver.close();
    }

    @Test
    void eventsShouldBeAggregatedAndPublishedToAggregateUpdateResultTopic() {
        Sequence firstEventSequence = Sequence.position(200);
        NonEmptyList<TestEvent> events = NonEmptyList.of(event1, event2);

        ConsumerRecord<byte[], byte[]> record = consumerRecordFactory.create(EVENTS_TOPIC, aggregateKey,
                buildSuccessCommandEvents(readSequence, firstEventSequence, events));

        topologyTestDriver.pipeInput(record);

        ProducerRecord<String, AggregateUpdate<Optional<TestAggregate>>> producerRecord = topologyTestDriver.readOutput(AGGREGATE_UPDATE_RESULT_TOPIC, keySerde.deserializer(), aggregateUpdateSerde.deserializer());

        verifyAggregatedEvents(producerRecord, firstEventSequence, events);
    }

    @Test
    void sequenceOfResultAggregateUpdateShouldBeTheSameAsSequenceOfLastEvent() {
        Sequence firstEventSequence = Sequence.position(200);
        NonEmptyList<TestEvent> events = NonEmptyList.of(event1, event2);

        ConsumerRecord<byte[], byte[]> record = consumerRecordFactory.create(EVENTS_TOPIC, aggregateKey,
                buildSuccessCommandEvents(readSequence, firstEventSequence, events));

        topologyTestDriver.pipeInput(record);
        ProducerRecord<String, AggregateUpdate<Optional<TestAggregate>>> producerRecord = topologyTestDriver.readOutput(AGGREGATE_UPDATE_RESULT_TOPIC, keySerde.deserializer(), aggregateUpdateSerde.deserializer());

        assertThat(producerRecord.value().sequence().getSeq()).isEqualTo(firstEventSequence.getSeq() + events.size()-1);
    }

    @Test
    void commandEventsWithFailureShouldNotWriteAggregateUpdateToResultStream() {
        CommandEvents<TestEvent, Optional<TestAggregate>> commandEvents = buildFailureCommandEvents(readSequence,
                NonEmptyList.of(CommandError.Reason.UnexpectedErrorCode));

        ConsumerRecord<byte[], byte[]> record = consumerRecordFactory.create(EVENTS_TOPIC, aggregateKey, commandEvents);
        topologyTestDriver.pipeInput(record);

        assertThat(topologyTestDriver.readOutput(AGGREGATE_UPDATE_RESULT_TOPIC, keySerde.deserializer(), aggregateUpdateSerde.deserializer()))
                .isNull();
    }

    private void verifyAggregatedEvents(ProducerRecord<String, AggregateUpdate<Optional<TestAggregate>>> producerRecord,
                                        Sequence firstEventSequence, NonEmptyList<TestEvent> events) {
        OutputVerifier.compareKeyValue(producerRecord, aggregateKey, new AggregateUpdate<>(
                Optional.of(new TestAggregate(String.join(", ", events.map(TestEvent::toString))))
                , Sequence.position(firstEventSequence.getSeq() + events.size()-1)));
    }

    private Aggregator<TestEvent, Optional<TestAggregate>> accumulatedEventNamesAggregator() {
        return (a, e) -> Optional.of(a.map(aa -> new TestAggregate(aa.name() + ", " + e.toString()))
                .orElse(new TestAggregate(e.toString())));
    }
}