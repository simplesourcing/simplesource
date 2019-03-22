package io.simplesource.kafka.internal.streams.topology;

import io.simplesource.kafka.internal.util.Tuple2;
import io.simplesource.api.UuidId;
import io.simplesource.kafka.spec.WindowSpec;
import lombok.Value;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.util.function.Function;

@Value
final class DistributorSerdes<K extends UuidId, V> {
    Serde<K> uuid;
    Serde<V> value;
}

@Value
final class DistributorContext<K extends UuidId, V> {
    public final String topicNameMapTopic;
    public final DistributorSerdes<K, V> serdes;
    private final WindowSpec responseWindowSpec;
    public final Function<V, K> idMapper;
}

final class ResultDistributor {

    static <K extends UuidId> KStream<K, String> resultTopicMapStream(DistributorContext<K, ?> ctx, final StreamsBuilder builder) {
        return builder.stream(ctx.topicNameMapTopic, Consumed.with(ctx.serdes().uuid(), Serdes.String()));
    }

    static <K extends UuidId, V> void distribute(DistributorContext<K, V> ctx, final KStream<?, V> resultStream, final KStream<K, String> topicNameStream) {

        DistributorSerdes<K, V> serdes = ctx.serdes();
        long retentionMillis = ctx.responseWindowSpec().retentionInSeconds() * 1000L;

        KStream<String, V> joined = resultStream.selectKey((k, v) -> ctx.idMapper.apply(v))
                .join(topicNameStream,
                        Tuple2::of,
                        JoinWindows.of(retentionMillis).until(retentionMillis * 2 + 1),
                        Joined.with(serdes.uuid(), serdes.value(), Serdes.String()))
                .map((uuid, tuple) -> KeyValue.pair(String.format("%s:%s", tuple.v2(), uuid.id().toString()), tuple.v1()));

        joined.to((key, value, context) -> key.substring(0, key.length() - 37), Produced.with(Serdes.String(), serdes.value()));
    }
}
