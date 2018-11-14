package io.simplesource.kafka.internal.streams.topology;

import io.simplesource.kafka.internal.util.Tuple;
import lombok.Value;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.util.UUID;
import java.util.function.Function;

@Value
final class DistributorSerdes<V> {
    Serde<UUID> uuid;
    Serde<V> value;
}

@Value
final class DistributorContext<V> {
    public final String topicNameMapTopic;
    public final DistributorSerdes<V> serdes;
    public final Function<V, UUID> idMapper;
}

final class ResultDistributor {

    static KStream<UUID, String> resultTopicMapStream(DistributorContext<?> ctx, final StreamsBuilder builder) {
        return builder.stream(ctx.topicNameMapTopic, Consumed.with(ctx.serdes().uuid(), Serdes.String()));
    }

    static <V> void distribute(DistributorContext<V> ctx, final KStream<?, V> resultStream, final KStream<UUID, String> topicNameStream) {

        DistributorSerdes<V> serdes = ctx.serdes();

        KTable<UUID, String> topicNameTable = topicNameStream
                .groupByKey(Serialized.with(serdes.uuid(), Serdes.String()))
                .reduce((t1, t2) -> t2, Materialized.with(serdes.uuid(), Serdes.String()));

        KStream<String, V> joined = resultStream.selectKey((k, v) -> ctx.idMapper.apply(v))
                .join(topicNameTable, Tuple::new, Joined.with(serdes.uuid(), serdes.value(), Serdes.String()))
                .map((uuid, tuple) -> KeyValue.pair(String.format("%s:%s", tuple.v2(), uuid.toString()), tuple.v1()));

        joined.to((key, value, context) -> key.substring(0, key.length() - 37), Produced.with(Serdes.String(), serdes.value()));
    }
}
