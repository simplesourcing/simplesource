package io.simplesource.kafka.serialization.avro2;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.simplesource.data.Sequence;
import io.simplesource.kafka.api.AggregateSerdes;
import io.simplesource.kafka.model.AggregateUpdate;
import io.simplesource.kafka.model.ValueWithSequence;
import io.simplesource.kafka.serialization.util.SerdeUtils;
import io.simplesource.serialization.avro.generated.AvroAggregateUpdate;
import io.simplesource.serialization.avro.generated.AvroValueWithSequence;
import org.apache.kafka.common.serialization.Serde;

import java.nio.ByteBuffer;

import static io.simplesource.kafka.serialization.avro2.AvroSerdeUtils.PAYLOAD_TYPE_AGGREGATE;
import static io.simplesource.kafka.serialization.avro2.AvroSerdeUtils.PAYLOAD_TYPE_EVENT;

final class AvroAggregateSerdes<K, C, E, A> extends AvroCommandSerdes<K, C> implements AggregateSerdes<K, C, E, A> {

    private final Serde<E> eventSerde;
    private final Serde<A> aggregateSerde;
    private final Serde<AvroAggregateUpdate> avroAggregateUpdateSerde;
    private final Serde<AvroValueWithSequence> avroValueWithSequenceSerde;

    AvroAggregateSerdes(final Serde<K> keySerde,
                        final Serde<C> commandSerde,
                        final Serde<E> eventSerde,
                        final Serde<A> aggregateSerde,
                        final String schemaRegistryUrl,
                        final boolean useMockSchemaRegistry) {
        super(keySerde, commandSerde, schemaRegistryUrl, useMockSchemaRegistry);
        SchemaRegistryClient regClient = useMockSchemaRegistry ? new MockSchemaRegistryClient() : null;
        avroAggregateUpdateSerde = SpecificSerdeUtils.specificAvroSerde(schemaRegistryUrl, false, regClient);
        avroValueWithSequenceSerde = SpecificSerdeUtils.specificAvroSerde(schemaRegistryUrl, false, regClient);
        this.eventSerde = eventSerde;
        this.aggregateSerde = aggregateSerde;
    }


    @Override
    public Serde<ValueWithSequence<E>> valueWithSequence() {
        return SerdeUtils.iMap(avroValueWithSequenceSerde,
                (topic, r) -> valueWithSequenceToAvro(eventSerde, topic, r),
                (topic, ar) -> valueWithSequenceFromAvro(eventSerde, topic, ar));
    }

    @Override
    public Serde<AggregateUpdate<A>> aggregateUpdate() {
        return SerdeUtils.iMap(avroAggregateUpdateSerde,
                (topic, r) -> aggregateUpdateToAvro(aggregateSerde, topic, r),
                (topic, ar) -> aggregateUpdateFromAvro(aggregateSerde, topic, ar));
    }

    static <E> AvroValueWithSequence valueWithSequenceToAvro(Serde<E> eventSerde, String topicName, ValueWithSequence<E> vs) {
        ByteBuffer serializedEvent = AvroSerdeUtils.serializePayload(eventSerde, topicName, vs.value(), PAYLOAD_TYPE_EVENT);
        return AvroValueWithSequence.newBuilder()
                .setSequence(vs.sequence().getSeq())
                .setValue(serializedEvent)
                .build();
    }

    static <E> ValueWithSequence<E> valueWithSequenceFromAvro(Serde<E> eventSerde, String topicName, AvroValueWithSequence ar) {
        E event = AvroSerdeUtils.deserializePayload(eventSerde, topicName, ar.getValue(), PAYLOAD_TYPE_EVENT);
        return new ValueWithSequence<>(event, Sequence.position(ar.getSequence()));
    }

    static <A> AvroAggregateUpdate aggregateUpdateToAvro(Serde<A> aggregateSerde, String topicName, AggregateUpdate<A> aggregateUpdate) {
        ByteBuffer serializedAggregate = AvroSerdeUtils.serializePayload(aggregateSerde, topicName, aggregateUpdate.aggregate(), PAYLOAD_TYPE_EVENT);
        return AvroAggregateUpdate.newBuilder()
                .setSequence(aggregateUpdate.sequence().getSeq())
                .setAggregate(serializedAggregate)
                .build();

    }

    static <A> AggregateUpdate<A> aggregateUpdateFromAvro(Serde<A> aggregateSerde, String topicName, AvroAggregateUpdate aau) {
        A aggregate = AvroSerdeUtils.deserializePayload(aggregateSerde, topicName, aau.getAggregate(), PAYLOAD_TYPE_AGGREGATE);
        return new AggregateUpdate<>(aggregate, Sequence.position(aau.getSequence()));

    }
}
