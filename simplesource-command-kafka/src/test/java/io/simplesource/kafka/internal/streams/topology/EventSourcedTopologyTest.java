package io.simplesource.kafka.internal.streams.topology;

import io.simplesource.kafka.api.AggregateResources;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("unchecked")
class EventSourcedTopologyTest {
    TopologyContext<String, TestCommand, TestEvent, Optional<TestAggregate>> context

    @BeforeEach
    void setUp() {
        context = new TestAggregateBuilder().buildContext();
    }

    @Test
    void ShouldDoSomething() {
        b = new TestAggregateBuilder()
                .withAggregator(accumulatedEventNamesAggregator())

        b.withAggregator()
    }

//    @Test
//    void shouldBuildTopologyFromSubTopologiesAndAggregateUpdateConsumers() {
//        StreamsBuilder streamsBuilder = mock(StreamsBuilder.class);
//        KStream commandRequestStream = mock(KStream.class);
//        KStream eventStream = mock(KStream.class);
//        KStream aggregateUpdateStream = mock(KStream.class);
//        when(streamsBuilder.stream(eq(commandRequestTopicName), any())).thenReturn(commandRequestStream);
//        when(commandProcessingSubTopology.add(commandRequestStream)).thenReturn(eventStream);
//        when(eventProcessingSubTopology.add(eventStream)).thenReturn(aggregateUpdateStream);
//
//        target.addTopology(streamsBuilder);
//
//        InOrder inOrder = inOrder(aggregateUpdatePublisher);
//        inOrder.verify(aggregateUpdatePublisher).toAggregateStore(aggregateUpdateStream);
//        inOrder.verify(aggregateUpdatePublisher).toCommandResultStore(aggregateUpdateStream);
//        inOrder.verify(aggregateUpdatePublisher).toCommandResponseTopic(aggregateUpdateStream);
//    }
//
//    @Test
//    void addTopologyShouldAddStateStoreForAggregateUpdate() {
//        StreamsBuilder streamsBuilder = mock(StreamsBuilder.class);
//        ArgumentCaptor<KeyValueStoreBuilder> storeBuilderArgumentCaptor = ArgumentCaptor.forClass(KeyValueStoreBuilder.class);
//
//        target.addTopology(streamsBuilder);
//
//        verify(streamsBuilder).addStateStore(storeBuilderArgumentCaptor.capture());
//        assertThat(storeBuilderArgumentCaptor.getValue().name()).isEqualTo(aggregateStateStoreName);
//    }
}