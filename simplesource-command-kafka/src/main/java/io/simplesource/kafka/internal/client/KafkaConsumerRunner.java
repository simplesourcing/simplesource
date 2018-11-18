package io.simplesource.kafka.internal.client;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

final class KafkaConsumerRunner {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerRunner.class);

    static private Properties copyProperties(Map<String, Object> properties) {
        Properties newProps = new Properties();
        properties.forEach((key, value) -> newProps.setProperty(key, value.toString()));
        return newProps;
    }

    static <R> ResponseSubscription run(
            Map<String, Object> properties,
            String topicName,
            Serde<R> responseSerde,
            BiConsumer<UUID, R> responseReceiver) {

        Properties consumerConfig  = copyProperties(properties);
        consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, String.format("response_consumer_%s", UUID.randomUUID().toString().substring(0, 8)));
        RunnableConsumer runnableConsumer = new RunnableConsumer<>(consumerConfig, responseSerde, topicName, responseReceiver);
        new Thread(runnableConsumer).start();
        return runnableConsumer::close;
    }

    static class RunnableConsumer<R> implements Runnable {
        private final KafkaConsumer<String, R> consumer;
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final String topicName;
        private final BiConsumer<UUID, R> receiver;

        RunnableConsumer(Properties consumerConfig, Serde<R> responseSerde, String topicName, BiConsumer<UUID, R> receiver) {
            this.topicName = topicName;
            this.receiver = receiver;
            consumer = new KafkaConsumer<>(consumerConfig, Serdes.String().deserializer(), responseSerde.deserializer());
        }

        @Override
        public void run() {
            try {
                consumer.subscribe(Collections.singletonList(topicName));
                while (!closed.get()) {
                    ConsumerRecords<String, R> records = consumer.poll(Duration.ofSeconds(1));
                    // Handle new records
                    records.iterator().forEachRemaining( record -> {
                        String recordKey = record.key();
                        // TODO factor this out
                        UUID id = UUID.fromString(record.key().substring(recordKey.length() - 36));
                        receiver.accept(id, record.value());
                    });
                }
            } catch (WakeupException e) {
                // Ignore exception if closing
                if (!closed.get()) throw e;
            } catch (Exception e) {
                logger.error(e.getMessage());
            } finally {
                consumer.close();
            }
        }

        void close() {
            closed.set(true);
            consumer.wakeup();
        }
    }

}
