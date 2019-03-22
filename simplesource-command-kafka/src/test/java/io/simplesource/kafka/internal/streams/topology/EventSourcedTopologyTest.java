package io.simplesource.kafka.internal.streams.topology;

import io.simplesource.api.CommandError;
import io.simplesource.api.CommandId;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Sequence;
import io.simplesource.kafka.dsl.InvalidSequenceStrategy;
import io.simplesource.kafka.internal.streams.MockInMemorySerde;
import io.simplesource.kafka.internal.streams.model.TestAggregate;
import io.simplesource.kafka.internal.streams.model.TestCommand;
import io.simplesource.kafka.internal.streams.model.TestEvent;
import io.simplesource.kafka.internal.streams.model.TestHandlers;
import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.kafka.model.CommandResponse;
import io.simplesource.kafka.model.ValueWithSequence;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("unchecked")
class EventSourcedTopologyTest {

    private TopologyTestDriver driver = null;
    private TestContextBuilder ctxBuilder = null;
    private static final String key = "key";

    @BeforeEach
    void setUp() {
        ctxBuilder = new TestContextBuilder()
                .withAggregator(TestHandlers.eventAggregator)
                .withCommandHandler(TestHandlers.commandHandler)
                .withInitialValue(key -> Optional.empty());
    }

    @AfterEach
    void tearDown() {
        if (driver != null) driver.close();
        MockInMemorySerde.resetCache();
    }

    @Test
    void invalidSequence() {
        TopologyContext<String, TestCommand, TestEvent, Optional<TestAggregate>> ctx = ctxBuilder.buildContext();
        driver = new TestDriverInitializer().build(builder -> EventSourcedTopology.addTopology(ctx, builder));

        TestContextDriver<String, TestCommand, TestEvent, Optional<TestAggregate>> ctxDriver = new TestContextDriver<>(ctx, driver);

        CommandRequest<String, TestCommand> commandRequest = new CommandRequest<>(
                CommandId.random(), key, Sequence.first().next(), new TestCommand.CreateCommand("Name"));

        ctxDriver.publishCommand( key, commandRequest);
        ctxDriver.verifyCommandResponse(key, r -> {
            assertThat(r.sequenceResult().isSuccess()).isEqualTo(false);
            assertThat(r.sequenceResult().failureReasons()).isEqualTo(
                    Optional.of(NonEmptyList.of(CommandError.of(CommandError.Reason.InvalidReadSequence, "Command received with read sequence 1 when expecting 0"))));
        });
        ctxDriver.verifyNoEvent();
        ctxDriver.verifyNoAggregateUpdate();
    }

    @Test
    void invalidCommand() {

        TopologyContext<String, TestCommand, TestEvent, Optional<TestAggregate>> ctx = ctxBuilder.buildContext();
        driver = new TestDriverInitializer().build(builder -> EventSourcedTopology.addTopology(ctx, builder));
        TestContextDriver<String, TestCommand, TestEvent, Optional<TestAggregate>> ctxDriver = new TestContextDriver<>(ctx, driver);

        CommandRequest<String, TestCommand> commandRequest = new CommandRequest<>(
                CommandId.random(), key, Sequence.first(), new TestCommand.UnsupportedCommand());

        ctxDriver.publishCommand( key, commandRequest);
        ctxDriver.verifyCommandResponse(key, r -> {
            assertThat(r.sequenceResult().isSuccess()).isEqualTo(false);
            assertThat(r.sequenceResult().failureReasons()).isEqualTo(
                    Optional.of(NonEmptyList.of(CommandError.of(CommandError.Reason.InvalidCommand, "Command not supported"))));
        });
        ctxDriver.verifyNoEvent();
        ctxDriver.verifyNoAggregateUpdate();
    }

    @Test
    void successfulCommandResponse() {

        TopologyContext<String, TestCommand, TestEvent, Optional<TestAggregate>> ctx = ctxBuilder.buildContext();
        driver = new TestDriverInitializer().build(builder -> EventSourcedTopology.addTopology(ctx, builder));
        TestContextDriver<String, TestCommand, TestEvent, Optional<TestAggregate>> ctxDriver = new TestContextDriver<>(ctx, driver);

        String name = "name";
        CommandRequest<String, TestCommand> commandRequest = new CommandRequest<>(
                CommandId.random(), key, Sequence.first(), new TestCommand.CreateCommand(name));

        ctxDriver.publishCommand( key, commandRequest);
        ctxDriver.verifyCommandResponse(key, v -> {
            assertThat(v.sequenceResult().isSuccess()).isEqualTo(true);
            assertThat(v.sequenceResult().getOrElse(Sequence.position(1000)).getSeq()).isEqualTo(Sequence.first().next().getSeq());
        });
        ctxDriver.verifyNoCommandResponse();

        ctxDriver.verifyEvent(key, e -> {
            assertThat(e.value()).isEqualTo(new TestEvent.Created(name));
            assertThat(e.sequence().getSeq()).isEqualTo(Sequence.first().next().getSeq());
        });
        ctxDriver.verifyNoEvent();

        ctxDriver.verifyAggregateUpdate(key, a -> {
            assertThat(a.sequence().getSeq()).isEqualTo(1L);
            assertThat(a.aggregate().get()).isEqualTo(new TestAggregate(name));
        });
        ctxDriver.verifyNoAggregateUpdate();
    }

    @Test
    void testMultipleUpdates() {

        TopologyContext<String, TestCommand, TestEvent, Optional<TestAggregate>> ctx = ctxBuilder.buildContext();
        driver = new TestDriverInitializer().build(builder -> EventSourcedTopology.addTopology(ctx, builder));
        TestContextDriver<String, TestCommand, TestEvent, Optional<TestAggregate>> ctxDriver = new TestContextDriver<>(ctx, driver);

        ctxDriver.publishCommand( key, new CommandRequest<>(
                CommandId.random(), key, Sequence.first(), new TestCommand.CreateCommand("firstName")));
        ctxDriver.verifyAggregateUpdate(key, null);
        CommandResponse<String> response = ctxDriver.verifyCommandResponse(key, null);
        ctxDriver.verifyEvents(key, null);

        for (int i = 0; i < 10; i++) {
            String newName = String.format("firstName %d", i);
            Sequence lastSequence = response.sequenceResult().getOrElse(Sequence.first());
            CommandRequest<String, TestCommand> commandRequest = new CommandRequest<>(
                    CommandId.random(), key, lastSequence, new TestCommand.UpdateWithNothingCommand(newName));
            ctxDriver.publishCommand(key, commandRequest);

            List<ValueWithSequence<TestEvent>> events = ctxDriver.verifyEvents(key, iV -> {
                Integer index = iV.v1();
                ValueWithSequence<TestEvent> value = iV.v2();
                assertThat(value.sequence().getSeq()).isEqualTo(index.longValue() + lastSequence.getSeq() + 1L);
            });
            assertThat(events.size()).isGreaterThan(1);
            long ms = 0L;
            for (ValueWithSequence<TestEvent> event: events) {
                long es = event.sequence().getSeq();
                if (ms < es) ms = es;
            }
            final long maxSequence = ms;

            response = ctxDriver.verifyCommandResponse(key, v -> {
                assertThat(v.sequenceResult().isSuccess()).isEqualTo(true);
                assertThat(v.sequenceResult().getOrElse(Sequence.first()).getSeq()).isEqualTo(maxSequence);
            });

            ctxDriver.verifyAggregateUpdate(key, update -> {
                assertThat(update.sequence().getSeq()).isEqualTo(maxSequence);
                assertThat(update.aggregate().isPresent()).isEqualTo(true);
                assertThat(update.aggregate().get().name()).isEqualTo(newName);
            });

            ctxDriver.verifyNoCommandResponse();
            ctxDriver.verifyNoAggregateUpdate();
        }
    }

    @Test
    void suppressInvalidSequenceCheck() {
        TopologyContext<String, TestCommand, TestEvent, Optional<TestAggregate>> ctx = ctxBuilder
                .withInvalidSequenceStrategy(InvalidSequenceStrategy.LastWriteWins)
                .buildContext();
        driver = new TestDriverInitializer().build(builder -> EventSourcedTopology.addTopology(ctx, builder));

        TestContextDriver<String, TestCommand, TestEvent, Optional<TestAggregate>> ctxDriver = new TestContextDriver<>(ctx, driver);

        CommandRequest<String, TestCommand> commandRequest = new CommandRequest<>(
                CommandId.random(), key, Sequence.position(1000), new TestCommand.CreateCommand("Name 2"));

        ctxDriver.publishCommand( key, commandRequest);
        ctxDriver.verifyCommandResponse(key, r -> {
            assertThat(r.sequenceResult().isSuccess()).isEqualTo(true);
            assertThat(r.sequenceResult().getOrElse(Sequence.position(2000))).isEqualTo(Sequence.first().next());
        });
        ctxDriver.verifyAggregateUpdate(key, r -> {
            assertThat(r.aggregate().get().name()).isEqualTo("Name 2");
        });
    }
    
    @Test
    void testIdempotence() {
        TopologyContext<String, TestCommand, TestEvent, Optional<TestAggregate>> ctx = ctxBuilder.buildContext();
        driver = new TestDriverInitializer().build(builder -> EventSourcedTopology.addTopology(ctx, builder));
        TestContextDriver<String, TestCommand, TestEvent, Optional<TestAggregate>> ctxDriver = new TestContextDriver<>(ctx, driver);

        CommandRequest<String, TestCommand> commandRequest = new CommandRequest<>(
                CommandId.random(), key, Sequence.first(), new TestCommand.CreateCommand("Name"));

        ctxDriver.publishCommand( key, commandRequest);

        CommandResponse<String> response = ctxDriver.verifyCommandResponse(key, r -> {
            assertThat(r.sequenceResult().getOrElse(Sequence.position(2000))).isEqualTo(Sequence.first().next());
        });

        ctxDriver.drainEvents();
        ctxDriver.drainAggregateUpdates();
        ctxDriver.drainCommandResponses();

        ctxDriver.publishCommand( key, commandRequest);
        ctxDriver.verifyNoEvent();
        ctxDriver.verifyNoAggregateUpdate();

        ctxDriver.verifyCommandResponse(key, r -> {
            assertThat(r.sequenceResult().getOrElse(Sequence.position(2000))).isEqualTo(Sequence.first().next());
            assertThat(r).isEqualToComparingFieldByField(response);
        });
    }

    @Test
    void testDistributor() {
        String topicNamesTopic = "topic_names";
        String outputTopic = "output_topic";

        TopologyContext<String, TestCommand, TestEvent, Optional<TestAggregate>> ctx = ctxBuilder.buildContext();
        driver = new TestDriverInitializer().build(builder -> {
            EventSourcedTopology.InputStreams<String, TestCommand> inputStreams = EventSourcedTopology.addTopology(ctx, builder);
            DistributorContext<CommandId, CommandResponse<String>> context = new DistributorContext<>(
                    topicNamesTopic,
                    new DistributorSerdes<>(ctx.serdes().commandResponseKey(), ctx.serdes().commandResponse()),
                    ctx.aggregateSpec().generation().stateStoreSpec(),
                    CommandResponse::commandId,
                    CommandId::id);

            KStream<CommandId, String> topicNames = builder.stream(topicNamesTopic, Consumed.with(ctx.serdes().commandResponseKey(), Serdes.String()));
            ResultDistributor.distribute(context, inputStreams.commandResponse, topicNames);
        });
        TestContextDriver<String, TestCommand, TestEvent, Optional<TestAggregate>> ctxDriver = new TestContextDriver<>(ctx, driver);

        CommandRequest<String, TestCommand> commandRequest = new CommandRequest<>(
                CommandId.random(), key, Sequence.first(), new TestCommand.CreateCommand("Name 2"));

        ctxDriver.getPublisher(ctx.serdes().commandResponseKey(), Serdes.String())
                .publish(topicNamesTopic, commandRequest.commandId(), outputTopic);
        ctxDriver.publishCommand( key, commandRequest);

        ProducerRecord<String, CommandResponse<String>> output = driver.readOutput(outputTopic,
                Serdes.String().deserializer(),
                ctx.serdes().commandResponse().deserializer());

        assertThat(output.key()).isEqualTo(String.format("%s:%s", outputTopic, commandRequest.commandId().id().toString()));
        assertThat(output.value().sequenceResult().isSuccess()).isEqualTo(true);
    }
}