package io.simplesource.kafka.internal.streams;

import io.simplesource.kafka.internal.client.Closeable;
import lombok.Value;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TopologyTestDriver;

import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

public final class TestTopologyReceiver<K, V> implements Closeable {
    Supplier<Integer> getDriverOutput;

    @Value
    public static final class ReceiverSpec<K, V> {
        String topicName;
        int delayMillis;
        int pollAttempts;
        Serde<V> valueSerde;
        Function<String, K> keyConverter;
    }

    public TestTopologyReceiver(BiConsumer<K, V> updateTarget, TopologyTestDriver driver, ReceiverSpec<K, V> spec) {
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

    public void pollForState() {
        getDriverOutput.get();
    }

    public void close() { }
}
