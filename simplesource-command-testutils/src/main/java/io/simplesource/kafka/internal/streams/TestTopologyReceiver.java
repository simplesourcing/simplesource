package io.simplesource.kafka.internal.streams;

import io.simplesource.kafka.internal.client.Closeable;
import lombok.Value;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.TopologyTestDriver;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public final class TestTopologyReceiver<K, V> implements Closeable {

    @Value
    public static final class ReceiverSpec<K, V> {
        String topicName;
        int delayMillis;
        int pollAttempts;
        Serde<K> keySerde;
        Serde<V> valueSerde;
    }

    public TestTopologyReceiver(BiConsumer<K, V> updateTarget, TopologyTestDriver driver, ReceiverSpec<K, V> spec) {

        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        ScheduledFuture<?> cancellable = scheduler.scheduleAtFixedRate(() -> {
            while (true) {
                ProducerRecord<K, V> record = driver.readOutput(spec.topicName,
                        spec.keySerde.deserializer(),
                        spec.valueSerde.deserializer());
                if (record == null)
                    break;
                updateTarget.accept(record.key(), record.value());
            }

        }, spec.delayMillis, spec.delayMillis, TimeUnit.MILLISECONDS);
        scheduler.schedule(
                () -> cancellable.cancel(true),
                spec.delayMillis * spec.pollAttempts + spec.delayMillis / 2,
                TimeUnit.MILLISECONDS);
    }

    public void close() { }
}
