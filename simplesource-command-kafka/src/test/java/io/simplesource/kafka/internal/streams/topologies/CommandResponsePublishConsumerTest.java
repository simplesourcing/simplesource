package io.simplesource.kafka.internal.streams.topologies;

import io.simplesource.api.CommandError;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.data.Sequence;
import io.simplesource.kafka.api.AggregateResources;
import io.simplesource.kafka.internal.MockedInMemorySerde;
import io.simplesource.kafka.internal.streams.model.TestAggregate;
import io.simplesource.kafka.model.AggregateUpdate;
import io.simplesource.kafka.model.AggregateUpdateResult;
import io.simplesource.kafka.model.CommandResponse;
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

import java.util.Optional;
import java.util.UUID;

import static io.simplesource.api.CommandError.Reason.UnexpectedErrorCode;
import static io.simplesource.kafka.internal.streams.topologies.EventSourcedTopologyTestUtility.aggregateUpdateResultWithSuccess;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CommandResponsePublishConsumerTest {
    private static final String AGGREGATE_UPDATE_RESULT_INPUT_TOPIC = "AGGREGATE_UPDATE_RESULT_INPUT_TOPIC";
    private static final String AGGREGATE_KEY = "Key";
    private static final String COMMAND_RESPONSE_TOPIC = "CommandResponse";

    @Mock
    private AggregateStreamResourceNames aggregateStreamResourceNames;
    private Serde<String> keySerde = Serdes.String();
    private Serde<CommandResponse> streamValueSerde = new MockedInMemorySerde<>();

    private TopologyTestDriver topologyTestDriver;
    private ConsumerRecordFactory<String, AggregateUpdateResult<Optional<TestAggregate>>> consumerRecordFactory;

    private CommandResponsePublishConsumer<String, Optional<TestAggregate>> target;

    @BeforeEach
    void setUp() {
        target = new CommandResponsePublishConsumer<>(aggregateStreamResourceNames, keySerde, streamValueSerde);

        when(aggregateStreamResourceNames.topicName(AggregateResources.TopicEntity.command_response)).thenReturn(COMMAND_RESPONSE_TOPIC);

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
    void successAggregateUpdateResultShouldBeMappedAndPassedThroughToCommandResponseTopic() {
        Sequence readSequence = Sequence.position(100);
        Sequence aggregateUpdateSequence = Sequence.position(200);
        AggregateUpdate<Optional<TestAggregate>> aggregateUpdate = new AggregateUpdate<>(Optional.empty(), aggregateUpdateSequence);
        AggregateUpdateResult<Optional<TestAggregate>> aggregateUpdateResult = aggregateUpdateResultWithSuccess(readSequence, aggregateUpdate);

        topologyTestDriver.pipeInput(consumerRecordFactory.create(AGGREGATE_UPDATE_RESULT_INPUT_TOPIC,
                AGGREGATE_KEY, aggregateUpdateResult));

        ProducerRecord<String, CommandResponse> record = topologyTestDriver.readOutput(COMMAND_RESPONSE_TOPIC, keySerde.deserializer(), streamValueSerde.deserializer());

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

        ProducerRecord<String, CommandResponse> record = topologyTestDriver.readOutput(COMMAND_RESPONSE_TOPIC, keySerde.deserializer(), streamValueSerde.deserializer());
        OutputVerifier.compareKeyValue(record, AGGREGATE_KEY, new CommandResponse(commandId, readSequence, Result.failure(commandErrors)));
    }
}