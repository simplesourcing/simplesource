package io.simplesource.kafka.testutils;

import io.simplesource.kafka.internal.client.ResponseSubscription;
import lombok.Value;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TopologyTestDriver;

import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

final class TestTopologyReceiver<K, V> implements ResponseSubscription {
    Supplier<Integer> getDriverOutput;

    @Value
    static final class ReceiverSpec<K, V> {
        String topicName;
        int delayMillis;
        int pollAttempts;
        Serde<V> valueSerde;
        Function<String, K> keyConverter;
    }

    TestTopologyReceiver(BiConsumer<K, V> updateTarget, TopologyTestDriver driver, ReceiverSpec<K, V> spec) {
        getDriverOutput = () -> {
            int count = 0;
            while (true) {
                ProducerRecord<String, V> record = driver.readOutput(spec.topicName,
                        Serdes.String().deserializer(),
                        spec.valueSerde.deserializer());
                if (record == null)
                    break;
                count++;
                K key = spec.keyConverter.apply(record.key());
                updateTarget.accept(key, record.value());
            }
            return count;
        };
    }

    void pollForState() {
        getDriverOutput.get();
    }

    public void close() { }
}
