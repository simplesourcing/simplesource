//package io.simplesource.kafka.internal.streams.topology;
//
//import io.simplesource.api.CommandError;
//import io.simplesource.data.NonEmptyList;
//import io.simplesource.data.Sequence;
//import io.simplesource.kafka.api.AggregateResources;
//import io.simplesource.kafka.api.AggregateResources.StateStoreEntity;
//import io.simplesource.kafka.internal.streams.MockInMemorySerde;
//import io.simplesource.kafka.internal.streams.model.TestAggregate;
//import io.simplesource.kafka.internal.streams.model.TestCommand;
//import io.simplesource.kafka.internal.streams.model.TestEvent;
//import io.simplesource.kafka.model.CommandRequest;
//import io.simplesource.kafka.model.ValueWithSequence;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.common.serialization.Serde;
//import org.apache.kafka.common.serialization.Serdes;
//import org.apache.kafka.streams.TopologyTestDriver;
//import org.apache.kafka.streams.test.ConsumerRecordFactory;
//import org.apache.kafka.streams.test.OutputVerifier;
//import org.junit.jupiter.api.AfterEach;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.api.extension.ExtendWith;
//import org.mockito.Mock;
//import org.mockito.junit.jupiter.MockitoExtension;
//import org.mockito.junit.jupiter.MockitoSettings;
//import org.mockito.quality.Strictness;
//
//import java.util.List;
//import java.util.Optional;
//import java.util.UUID;
//
//import static io.simplesource.kafka.internal.streams.topologies.EventSourcedTopologyTestUtility.buildFailureCommandEvents;
//import static io.simplesource.kafka.internal.streams.topologies.EventSourcedTopologyTestUtility.buildSuccessCommandEvents;
//import static org.assertj.core.api.Assertions.assertThat;
//import static org.mockito.Mockito.when;
//
//@ExtendWith(MockitoExtension.class)
//@MockitoSettings(strictness = Strictness.LENIENT)
//class CommandProcessingSubTopologyTest {
//    private static final String NAME = "Name";
//    private static final String AGGREGATE_KEY = "key";
//
//    @Mock
//    private CommandRequestTransformer<String, TestCommand, TestEvent, Optional<TestAggregate>> commandRequestTransformer;
//
//    private Serde<String> keySerde = Serdes.String();
//    private Serde<CommandRequest<TestCommand>> streamValueSerde = new MockInMemorySerde<>();
//    private Serde<ValueWithSequence<TestEvent>> valueWithSequenceSerde = new MockInMemorySerde<>();
//
//    private ConsumerRecordFactory<String, CommandRequest<TestCommand>> consumerRecordFactory;
//    private TopologyTestDriver topologyTestDriver;
//    private String commandRequestTopicName = "command_request";
//    private String resultEventsTopicName = TestContextBuilder.topicName(AggregateResources.TopicEntity.event);
//    private String aggregateUpdateStateStoreName = TestContextBuilder.stateStoreName(StateStoreEntity.aggregate_update);
//
//    private CommandProcessingSubTopology<String, TestCommand, TestEvent, Optional<TestAggregate>> target;
//
//    @BeforeEach
//    void setUp() {
//        TopologyContext<String, TestCommand, TestEvent, Optional<TestAggregate>> context =
//                new TestContextBuilder()
//                        .buildContext();
//
//        target = new CommandProcessingSubTopology<>(context, commandRequestTransformer);
//
//        topologyTestDriver = new TopologyTestDriverInitializer()
//                .withStateStore(aggregateUpdateStateStoreName, keySerde, streamValueSerde)
//                .withSourceTopicName(commandRequestTopicName)
//                .<String, CommandRequest<TestCommand>>build(s -> target.add(s));
//
//        consumerRecordFactory = new ConsumerRecordFactory<>(keySerde.serializer(),
//                streamValueSerde.serializer());
//    }
//
//    @AfterEach
//    void tearDown() {
//        topologyTestDriver.close();
//        MockInMemorySerde.resetCache();
//    }
//
//    @Test
//    void shouldWriteResultEventsToResultTopicWhenCommandRequestProcessingReturnEvents() {
//        Sequence readSequence = Sequence.position(100);
//        NonEmptyList<TestEvent> events = NonEmptyList.of(new TestEvent.Created("Event1"),
//                new TestEvent.Created("Event2"));
//        CommandRequest<TestCommand> commandRequest = new CommandRequest<>(
//                new TestCommand.CreateCommand(NAME), readSequence, UUID.randomUUID());
//
//        commandRequestProcessingResultInEvents(AGGREGATE_KEY, commandRequest, events);
//
//        ConsumerRecord<byte[], byte[]> record = consumerRecordFactory.create(commandRequestTopicName, AGGREGATE_KEY, commandRequest);
//        topologyTestDriver.pipeInput(record);
//
//        verifyResultEvents(AGGREGATE_KEY, readSequence, events);
//    }
//
//    @Test
//    void shouldNotWriteAnyEventsToResultTopicWhenCommandRequestProcessingReturnError() {
//        Sequence readSequence = Sequence.position(100);
//        CommandRequest<TestCommand> commandRequest = new CommandRequest<>(
//                new TestCommand.CreateCommand(NAME), readSequence, UUID.randomUUID());
//
//        commandRequestProcessingResultInErrors(AGGREGATE_KEY, commandRequest, NonEmptyList.of(CommandError.Reason.Timeout));
//
//        ConsumerRecord<byte[], byte[]> record = consumerRecordFactory.create(commandRequestTopicName, AGGREGATE_KEY, commandRequest);
//        topologyTestDriver.pipeInput(record);
//
//        assertThat(topologyTestDriver.readOutput(resultEventsTopicName, keySerde.deserializer(), valueWithSequenceSerde.deserializer()))
//                .isNull();
//    }
//
//    private void commandRequestProcessingResultInEvents(String key, CommandRequest<TestCommand> request, NonEmptyList<TestEvent> events) {
//        CommandEvents<TestEvent, Optional<TestAggregate>> commandEvents =
//                buildSuccessCommandEvents(request.commandId(), request.readSequence(), events);
//
//        when(commandRequestTransformer.transform(key, request)).thenReturn(commandEvents);
//    }
//
//    private void commandRequestProcessingResultInErrors(String key, CommandRequest<TestCommand> request, NonEmptyList<CommandError.Reason> reasons) {
//        CommandEvents<TestEvent, Optional<TestAggregate>> commandEvents =
//                buildFailureCommandEvents(request.commandId(), request.readSequence(), reasons);
//
//        when(commandRequestTransformer.transform(key, request)).thenReturn(commandEvents);
//    }
//
//    private void verifyResultEvents(String key, Sequence readSequence, List<TestEvent> events) {
//        Sequence[] sequences = new Sequence[]{readSequence};
//
//        events.forEach(e -> {
//            sequences[0] = sequences[0].next();
//            ProducerRecord<String, ValueWithSequence<TestEvent>> outputRecord = topologyTestDriver.readOutput(resultEventsTopicName,
//                    keySerde.deserializer(), valueWithSequenceSerde.deserializer());
//            OutputVerifier.compareKeyValue(outputRecord, key, new ValueWithSequence<>(e, sequences[0]));
//        });
//    }
//}