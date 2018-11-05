package io.simplesource.kafka.internal.streams.topology;

import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.data.Sequence;
import io.simplesource.kafka.api.AggregateResources;
import io.simplesource.kafka.internal.streams.MockInMemorySerde;
import io.simplesource.kafka.internal.streams.model.TestAggregate;
import io.simplesource.kafka.internal.streams.model.TestCommand;
import io.simplesource.kafka.internal.streams.model.TestEvent;
import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.kafka.model.CommandResponse;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import static org.assertj.core.api.Assertions.*;


@ExtendWith(MockitoExtension.class)
@SuppressWarnings("unchecked")
class EventSourcedTopologyTest {

    TopologyTestDriver driver = null;
    TestContextBuilder ctxBuilder = null;

    @BeforeEach
    void setUp() {
        ctxBuilder = new TestContextBuilder()
                .withAggregator((currentAggregate, event) -> Optional.of(new TestAggregate("NAME")))
                .withCommandHandler((
                        key,
                        currentAggregate,
                        command) -> Result.success(NonEmptyList.of(new TestEvent.Created("name"))))
                .withInitialValue(key -> Optional.empty());
    }

    @AfterEach
    void tearDown() {
        if (driver != null) driver.close();
        MockInMemorySerde.resetCache();
    }

    @Test
    void testSuccessfulCommandResponse() {
        TopologyContext<String, TestCommand, TestEvent, Optional<TestAggregate>> ctx = ctxBuilder.buildContext();
        TopologyTestDriver driver = new TopologyTestDriverInitializer().build(builder -> {
            EventSourcedTopology.addTopology(ctx, builder);
        });

        CommandRequest<TestCommand> commandRequest = new CommandRequest<>(
                new TestCommand.CreateCommand("name"), Sequence.first(), UUID.randomUUID());

        new TestDriverPublisher<>(driver, ctx.serdes().aggregateKey(), ctx.serdes().commandRequest()).publish(
                ctx.topicName(AggregateResources.TopicEntity.command_request), "key", commandRequest);

        ProducerRecord<String, CommandResponse> response = driver.readOutput(ctx.topicName(AggregateResources.TopicEntity.command_response),
                ctx.serdes().aggregateKey().deserializer(),
                ctx.serdes().commandResponse().deserializer());

        assertThat(response.key()).isEqualTo("key");
        assertThat(response.value().sequenceResult().isSuccess()).isEqualTo(true);
        assertThat(response.value().sequenceResult().getOrElse(Sequence.position(1000)).getSeq()).isEqualTo(Sequence.first().next().getSeq());
    }

}