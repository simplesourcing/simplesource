package io.simplesource.kafka.internal.streams.topology;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;

class TestDriverPublisher<K, V> {
    private final ConsumerRecordFactory<K, V> factory;
    private final TopologyTestDriver driver;

    TestDriverPublisher(final TopologyTestDriver driver, final Serde<K> keySerde, final Serde<V> valueSerde) {
        this.driver = driver;
        factory = new ConsumerRecordFactory<>(keySerde.serializer(), valueSerde.serializer());
    }

    private ConsumerRecordFactory<K, V> recordFactory() {
        return factory;
    }

    void publish(final String topic, final K key, V value) {
        driver.pipeInput(recordFactory().create(topic, key, value));
    }
}
