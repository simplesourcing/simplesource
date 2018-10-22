package io.simplesource.kafka.internal.streams.topologies;

import io.simplesource.data.NonEmptyList;
import io.simplesource.kafka.api.AggregateSerdes;
import io.simplesource.kafka.internal.streams.model.TestAggregate;
import io.simplesource.kafka.internal.streams.model.TestCommand;
import io.simplesource.kafka.internal.streams.model.TestEvent;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static io.simplesource.kafka.api.AggregateResources.StateStoreEntity.aggregate_update;
import static io.simplesource.kafka.api.AggregateResources.TopicEntity.command_request;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("unchecked")
class EventSourcedTopologyTest {
    private static final String AGGREGATE_UPDATE_STORE = "AggregateUpdateStore";
    private static final String COMMAND_REQUEST_TOPIC = "CommandRequestTopic";

    private EventSourcedTopology<String, TestCommand, TestEvent, Optional<TestAggregate>> target;
    @Mock
    private AggregateSerdes<String, TestCommand, TestEvent, Optional<TestAggregate>> aggregateSerdes;
    @Mock
    private AggregateStreamResourceNames aggregateStreamResourceNames;
    @Mock
    private CommandProcessingSubTopology<String, TestCommand, TestEvent, Optional<TestAggregate>> commandProcessingSubTopology;
    @Mock
    private EventProcessingSubTopology<String, TestEvent, Optional<TestAggregate>> eventProcessingSubTopology;
    @Mock
    private AggregateUpdateResultStreamConsumer aggregateUpdateConsumer1;
    @Mock
    private AggregateUpdateResultStreamConsumer aggregateUpdateConsumer2;

    @BeforeEach
    void setUp() {
        target = new EventSourcedTopology<>(aggregateSerdes, aggregateStreamResourceNames, commandProcessingSubTopology,
                eventProcessingSubTopology, NonEmptyList.of(aggregateUpdateConsumer1, aggregateUpdateConsumer2));

        when(aggregateStreamResourceNames.stateStoreName(aggregate_update)).thenReturn(AGGREGATE_UPDATE_STORE);
        when(aggregateStreamResourceNames.topicName(command_request)).thenReturn(COMMAND_REQUEST_TOPIC);
    }

    @Test
    void shouldBuildTopologyFromSubTopologiesAndAggregateUpdateConsumers() {
        StreamsBuilder streamsBuilder = mock(StreamsBuilder.class);
        KStream commandRequestStream = mock(KStream.class);
        KStream eventStream = mock(KStream.class);
        KStream aggregateUpdateStream = mock(KStream.class);
        when(streamsBuilder.stream(eq(COMMAND_REQUEST_TOPIC), any())).thenReturn(commandRequestStream);
        when(commandProcessingSubTopology.add(commandRequestStream)).thenReturn(eventStream);
        when(eventProcessingSubTopology.add(eventStream)).thenReturn(aggregateUpdateStream);

        target.addTopology(streamsBuilder);

        InOrder inOrder = inOrder(aggregateUpdateConsumer1, aggregateUpdateConsumer2);
        inOrder.verify(aggregateUpdateConsumer1).accept(aggregateUpdateStream);
        inOrder.verify(aggregateUpdateConsumer2).accept(aggregateUpdateStream);
    }

    @Test
    void addTopologyShouldAddStateStoreForAggregateUpdate() {
        StreamsBuilder streamsBuilder = mock(StreamsBuilder.class);
        ArgumentCaptor<KeyValueStoreBuilder> storeBuilderArgumentCaptor = ArgumentCaptor.forClass(KeyValueStoreBuilder.class);

        target.addTopology(streamsBuilder);

        verify(streamsBuilder).addStateStore(storeBuilderArgumentCaptor.capture());
        assertThat(storeBuilderArgumentCaptor.getValue().name()).isEqualTo(AGGREGATE_UPDATE_STORE);

    }
}