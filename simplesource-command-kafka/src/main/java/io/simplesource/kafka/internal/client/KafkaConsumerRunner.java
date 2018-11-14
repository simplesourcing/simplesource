package io.simplesource.kafka.internal.client;

import io.simplesource.kafka.model.CommandResponse;
import io.simplesource.kafka.spec.CommandSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

class KafkaConsumerRunner implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerRunner.class);

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final KafkaConsumer<String, CommandResponse> consumer;
    private final String topicName;
    private final ConcurrentHashMap<UUID, KafkaCommandAPI.ResponseHandlers> handlerMap;

    private Properties copyProperties(Map<String, Object> properties) {
        Properties newProps = new Properties();
        properties.forEach((key, value) -> newProps.setProperty(key, value.toString()));
        return newProps;
    }

    KafkaConsumerRunner(Map<String, Object> properties, String topicName, CommandSpec<?, ?> commandSpec, ConcurrentHashMap<UUID, KafkaCommandAPI.ResponseHandlers> handlerMap) {

        Properties consumerConfig  = copyProperties(properties);
        //consumerConfig.putAll(spec.config)
        consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, String.format("response_consumer_%s", UUID.randomUUID().toString().substring(0, 8)));
        // For now automatic - probably rather do this manually
        consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumerConfig.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        consumerConfig.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        this.topicName = topicName;
        this.handlerMap = handlerMap;
        consumer = new KafkaConsumer<>(consumerConfig, Serdes.String().deserializer(), commandSpec.serdes().commandResponse().deserializer());
    }

    public void run() {
        try {
            consumer.subscribe(Collections.singletonList(topicName));
            while (!closed.get()) {
                ConsumerRecords<String, CommandResponse> records = consumer.poll(10000);
                // Handle new records
                records.iterator().forEachRemaining( record -> {
                    String recordKey = record.key();
                    UUID id = UUID.fromString(record.key().substring(recordKey.length() - 36));
                    KafkaCommandAPI.ResponseHandlers responseHandler = handlerMap.getOrDefault(id, null);
                    if (responseHandler != null) {
                        responseHandler.handlers().forEach(future -> {
                            future.complete(record.value());
                        });
                        handlerMap.remove(id);
                    }
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

    // Shutdown hook which can be called from a separate thread
    void close() {
        closed.set(true);
        consumer.wakeup();
    }
}
