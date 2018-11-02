package io.simplesource.kafka.internal.streams.topologies;

import io.simplesource.kafka.internal.MockInMemorySerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;
import java.util.function.Consumer;

class TopologyTestDriverInitializer {
    private StreamsBuilder streamsBuilder;
    private Topology topology;
    private String sourceTopicName = "DummyTopicName";

    TopologyTestDriverInitializer() {
        streamsBuilder = new StreamsBuilder();
        topology = streamsBuilder.build();
    }

    TopologyTestDriverInitializer withSourceTopicName(final String sourceTopicName) {
        this.sourceTopicName = sourceTopicName;
        return this;
    }

    TopologyTestDriverInitializer withStateStore(String stateStoreName, Serde<?> keySerde, Serde<?> valueSerde) {
        topology.addStateStore(
                Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(stateStoreName),
                        keySerde, valueSerde)
                        .withLoggingDisabled());
        return this;
    }

    <K, V> TopologyTestDriver build(Consumer<KStream<K, V>> streamConsumer) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, MockInMemorySerde.class);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        Topology topology = streamsBuilder.build();

        streamConsumer.accept(streamsBuilder.stream(sourceTopicName));
        return new TopologyTestDriver(topology, props);
    }
}