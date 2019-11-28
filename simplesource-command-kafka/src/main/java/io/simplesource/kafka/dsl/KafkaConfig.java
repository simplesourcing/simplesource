package io.simplesource.kafka.dsl;

import lombok.Value;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;

import java.util.*;

import static java.util.Objects.requireNonNull;

@Value
public class KafkaConfig {
    private final Properties config;

    public String applicationId() {
        return (String)config.get(StreamsConfig.APPLICATION_ID_CONFIG);
    }

    public String bootstrapServers() {
        return (String)config.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
    }

    public String stateDir() {
        return (String)config.get(StreamsConfig.STATE_DIR_CONFIG);
    }

    public boolean isExactlyOnce() {
        return Objects.equals(
            config.get(StreamsConfig.PROCESSING_GUARANTEE_CONFIG),
            StreamsConfig.EXACTLY_ONCE);
    }

    public Map<String, Object> adminClientConfig() {
        return Collections.singletonMap(
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    }

    public Map<String, Object> consumerConfig() {
        final Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        configs.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        configs.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        return configs;
    }

    public Map<String, Object> producerConfig() {
        final Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        configs.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.get(ProducerConfig.COMPRESSION_TYPE_CONFIG));
        if (isExactlyOnce()) {
            configs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
            configs.put(ProducerConfig.RETRIES_CONFIG, 3);
            configs.put(ProducerConfig.ACKS_CONFIG, "all");
        }
        return configs;
    }

    public Properties streamsConfig() {
        return config;
    }

    public static class Builder {
        private Properties config = new Properties();

        public Builder() {
            config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
            config.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");
            config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        }

        public Builder withKafkaApplicationId(final String applicationId) {
            config.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
            return this;
        }

        public Builder withKafkaBootstrap(final String bootstrapServers) {
            config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            return this;
        }

        public Builder withExactlyOnce() {
            config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
            return this;
        }

        public Builder withAtLeastOnce() {
            config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
            return this;
        }

        public Builder withReplicationFactor(final int replicationFactor) {
            if (replicationFactor < 1) {
                throw new RuntimeException("Invalid replication factor");
            }

            config.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, String.valueOf(replicationFactor));
            return this;
        }

        public Builder withSetting(final String name, final Object value) {
            config.put(name, value);
            return this;
        }

        public Builder withSettings(final Map<String, Object> config) {
            this.config.putAll(config);
            return this;
        }

        public KafkaConfig build() {
            validateKafkaConfig(false);
            return new KafkaConfig(config);
        }

        public KafkaConfig build(boolean clientOnly) {
            validateKafkaConfig(clientOnly);
            return new KafkaConfig(config);
        }

        private void validateKafkaConfig(boolean clientOnly) {
            (clientOnly ? Arrays.asList(
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG
            ) :
            Arrays.asList(
                StreamsConfig.APPLICATION_ID_CONFIG,
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG
            )).forEach(key ->
                requireNonNull(config.get(key), "KafkaConfig missing " + key));
        }

    }

}
