package io.simplesource.kafka.internal.streams.topology;

import io.simplesource.kafka.internal.streams.MockInMemorySerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;
import java.util.function.Consumer;

class TopologyTestDriverInitializer {
    private StreamsBuilder streamsBuilder;

    TopologyTestDriverInitializer() {
        streamsBuilder = new StreamsBuilder();
    }

    TopologyTestDriverInitializer withStateStore(String stateStoreName, Serde<?> keySerde, Serde<?> valueSerde) {
        streamsBuilder.addStateStore(
                Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(stateStoreName),
                        keySerde, valueSerde)
                        .withLoggingDisabled());
        return this;
    }

    TopologyTestDriver build(Consumer<StreamsBuilder> builderConsumer) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        Topology topology = streamsBuilder.build();

        builderConsumer.accept(streamsBuilder);
        return new TopologyTestDriver(topology, props);
    }
}
