package io.simplesource.kafka.internal.streams.topology;

import io.simplesource.api.Aggregator;
import io.simplesource.api.CommandHandler;
import io.simplesource.api.InitialValue;
import io.simplesource.kafka.api.AggregateResources;
import io.simplesource.kafka.api.AggregateSerdes;
import io.simplesource.kafka.dsl.AggregateBuilder;
import io.simplesource.kafka.dsl.InvalidSequenceStrategy;
import io.simplesource.kafka.internal.streams.MockInMemorySerde;
import io.simplesource.kafka.util.PrefixResourceNamingStrategy;
import io.simplesource.kafka.internal.streams.model.TestAggregate;
import io.simplesource.kafka.internal.streams.model.TestCommand;
import io.simplesource.kafka.internal.streams.model.TestEvent;
import io.simplesource.kafka.model.*;
import io.simplesource.kafka.spec.TopicSpec;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

class TestContextBuilder {

    private static final String AGGREGATE_NAME = "testaggregate";
    private static final PrefixResourceNamingStrategy RESOURCE_NAMING_STRATEGY = new PrefixResourceNamingStrategy("", "-");
    private CommandHandler<String, TestCommand, TestEvent, Optional<TestAggregate>> commandHandler;
    private Aggregator<TestEvent, Optional<TestAggregate>> eventAggregator;
    private InitialValue<String, Optional<TestAggregate>> initialValue;
    private InvalidSequenceStrategy invalidSequenceStrategy = InvalidSequenceStrategy.Strict;

    TestContextBuilder() {
        eventAggregator = (a, e) -> {
            throw new RuntimeException("Event aggregator is required, yet not configured");
        };
        commandHandler = (k, a, c) -> {
            throw new RuntimeException("Command handler is required, yet not configured");
        };
        initialValue = k -> Optional.empty();
    }
    public TopologyContext<String, TestCommand, TestEvent, Optional<TestAggregate>> buildContext() {
        AggregateBuilder<String, TestCommand, TestEvent, Optional<TestAggregate>> aggregateBuilder =
                AggregateBuilder.<String, TestCommand, TestEvent, Optional<TestAggregate>>newBuilder()
                        .withName(AGGREGATE_NAME)
                        .withSerdes(aggregateSerdes())
                        .withCommandHandler(commandHandler)
                        .withInitialValue(initialValue)
                        .withCommandHandler(commandHandler)
                        .withAggregator(eventAggregator)
                        .withInvalidSequenceStrategy(invalidSequenceStrategy)
                        .withResourceNamingStrategy(RESOURCE_NAMING_STRATEGY);
        configureTopicSpec(aggregateBuilder);

        return new TopologyContext<>(aggregateBuilder.build());
    }

    public TestContextBuilder withCommandHandler(CommandHandler<String, TestCommand, TestEvent, Optional<TestAggregate>> commandHandler) {
        this.commandHandler = commandHandler;
        return this;
    }

    public TestContextBuilder withInitialValue(InitialValue<String, Optional<TestAggregate>> initialValue) {
        this.initialValue = initialValue;
        return this;
    }

    public TestContextBuilder withAggregator(Aggregator<TestEvent, Optional<TestAggregate>> eventAggregator) {
        this.eventAggregator = eventAggregator;
        return this;
    }

    public TestContextBuilder withInvalidSequenceStrategy(InvalidSequenceStrategy invalidSequenceStrategy) {
        this.invalidSequenceStrategy = invalidSequenceStrategy;
        return this;
    }

    private void configureTopicSpec(AggregateBuilder<String, TestCommand, TestEvent, Optional<TestAggregate>> aggregateBuilder) {
        TopicSpec defaultTopicSpec = new TopicSpec(1, Short.valueOf("1"), Collections.emptyMap());

        Arrays.stream(AggregateResources.TopicEntity.values())
                .forEach(t -> aggregateBuilder.withTopicSpec(t, defaultTopicSpec));
    }

    private static AggregateSerdes<String, TestCommand, TestEvent, Optional<TestAggregate>> aggregateSerdes() {
        return new AggregateSerdes<String, TestCommand, TestEvent, Optional<TestAggregate>>() {
            @Override
            public Serde<String> aggregateKey() {
                return Serdes.String();
            }

            @Override
            public Serde<CommandRequest<String, TestCommand>> commandRequest() {
                return new MockInMemorySerde<>();
            }

            @Override
            public Serde<UUID> commandResponseKey() {
                return new MockInMemorySerde<>();
            }

            @Override
            public Serde<ValueWithSequence<TestEvent>> valueWithSequence() {
                return new MockInMemorySerde<>();
            }

            @Override
            public Serde<AggregateUpdate<Optional<TestAggregate>>> aggregateUpdate() {
                return new MockInMemorySerde<>();
            }

            @Override
            public Serde<CommandResponse> commandResponse() {
                return new MockInMemorySerde<>();
            }
        };
    }

    public static String topicName(AggregateResources.TopicEntity entity) {
        return RESOURCE_NAMING_STRATEGY.topicName(AGGREGATE_NAME, entity.name());
    }
}