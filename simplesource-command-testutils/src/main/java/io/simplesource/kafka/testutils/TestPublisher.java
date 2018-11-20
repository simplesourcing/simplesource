package io.simplesource.kafka.testutils;

import io.simplesource.data.FutureResult;
import io.simplesource.kafka.internal.client.RequestPublisher;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;

import java.time.Instant;

class TestPublisher<K, V> implements RequestPublisher<K, V> {

    private final ConsumerRecordFactory<K,V> factory;
    TopologyTestDriver driver;
    private final String topicName;

    TestPublisher(TopologyTestDriver driver, final Serde<K> keySerde, final Serde<V> valueSerde, String topicName) {

        this.driver = driver;
        this.topicName = topicName;
        factory = new ConsumerRecordFactory<>(keySerde.serializer(), valueSerde.serializer());
    }

    @Override
    public FutureResult<Exception, PublishResult> publish(K key, V value) {
        driver.pipeInput(factory.create(topicName, key, value));
        return FutureResult.of(new PublishResult(Instant.now().getEpochSecond()));
    }
}
