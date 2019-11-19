package io.simplesource.kafka.serialization.avro2;

import org.apache.kafka.common.serialization.Serde;

import java.nio.ByteBuffer;

final class AvroSerdeUtils {
    private static String PAYLOAD_TOPIC_SUFFIX = "payload";
    static String PAYLOAD_TYPE_KEY = "aggregate-key";
    static String PAYLOAD_TYPE_COMMAND = "command";
    static String PAYLOAD_TYPE_EVENT = "event";
    static String PAYLOAD_TYPE_AGGREGATE = "aggregate";

    static <A> ByteBuffer serializePayload(Serde<A> payloadSerde, String payloadTopic, A payload, String payloadType) {
        return ByteBuffer.wrap(payloadSerde.serializer().serialize(getSubjectName(payloadTopic, payloadType), payload));
    }

    static <A> A deserializePayload(Serde<A> payloadSerde, String payloadTopic, ByteBuffer payloadBytes, String payloadType) {
        return payloadSerde.deserializer().deserialize(getSubjectName(payloadTopic, payloadType), payloadBytes.array());
    }

    static String getSubjectName(String topic, String actionType) {
        String normalisedActionType = actionType.toLowerCase().replace(" ", "_");
        return String.format("%s-%s-%s", topic, normalisedActionType, PAYLOAD_TOPIC_SUFFIX);
    }
}
