package io.simplesource.kafka.dsl;

import lombok.Value;
import io.simplesource.kafka.internal.client.ClientConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.HostInfo;

import java.util.*;

import static java.util.Objects.requireNonNull;

@Value
public final class KafkaConfig {
    private final Map<String, Object> config;
    private final ClientConfig clientConfig;

    public HostInfo currentHostInfo() {
        return new HostInfo(clientConfig.iface(), clientConfig.port());
    }

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

    public Map<String, Object> streamsConfig() {
        return new HashMap<>(config);
    }

    public static class Builder {
        private Map<String, Object> config = new HashMap<>();

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

        public Builder withApplicationServer(final String applicationServer) {
            config.put(StreamsConfig.APPLICATION_SERVER_CONFIG, applicationServer);
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


        public Builder withSetting(final String name, final Object value) {
            config.put(name, value);
            return this;
        }

        public Builder withSettings(final Map<String, Object> config) {
            this.config.putAll(config);
            return this;
        }

        public KafkaConfig build() {
            validateKafkaConfig();
            final String rpc = String.valueOf(config.get(StreamsConfig.APPLICATION_SERVER_CONFIG));
            final int index = rpc.indexOf(':');
            if (index < 0) {
                throw new IllegalArgumentException(StreamsConfig.APPLICATION_SERVER_CONFIG + " must be in format host:port");
            }
            final String rpcHost = rpc.substring(0, index);
            final int rpcPort = Integer.parseInt(rpc.substring(index + 1));

            //TODO parse rest of cluster properties
            ClientConfig clientConfig = new ClientConfig();
            clientConfig.iface(rpcHost);
            clientConfig.port(rpcPort);

            return new KafkaConfig(config, clientConfig);
        }

        private void validateKafkaConfig() {
            Arrays.asList(
                StreamsConfig.APPLICATION_ID_CONFIG,
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                StreamsConfig.APPLICATION_SERVER_CONFIG
            ).forEach(key ->
                requireNonNull(config.get(key), "KafkaConfig missing " + key));
        }

    }

}
