package io.simplesource.kafka.serialization.avro.mappers;

import io.simplesource.api.CommandError;
import io.simplesource.api.CommandId;
import io.simplesource.data.Result;
import io.simplesource.data.Sequence;
import io.simplesource.kafka.api.AggregateSerdes;
import io.simplesource.kafka.api.EventSerdes;
import io.simplesource.kafka.model.AggregateUpdate;
import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.kafka.model.CommandResponse;
import io.simplesource.kafka.model.ValueWithSequence;
import io.simplesource.kafka.serialization.avro.AvroSerdes;
import io.simplesource.kafka.serialization.avro.generated.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class AvroGenericEventSerdeTests {
    private static final String topic = "topic";
    private EventSerdes<UserAccountId, AccountEvent> serdes;

    @BeforeEach
    void setup() {
        serdes = AvroSerdes.Generic.event(
                "http://localhost:8081",
                true);
    }

    @Test
    void aggregateKey() {
        UserAccountId aggKey = new UserAccountId("userId");
        byte[] serialised = serdes.aggregateKey().serializer().serialize(topic, aggKey);
        UserAccountId deserialised = serdes.aggregateKey().deserializer().deserialize(topic, serialised);
        assertThat(deserialised).isEqualToComparingFieldByField(aggKey);
    }

    @Test
    void uuidResponseKey() {
        CommandId responseKey = CommandId.random();

        byte[] serialised = serdes.commandId().serializer().serialize(topic, responseKey);
        CommandId deserialised = serdes.commandId().deserializer().deserialize(topic, serialised);
        assertThat(deserialised).isEqualTo(responseKey);
    }

    @Test
    void aggregateUpdate() {
        AggregateUpdate<Boolean> update = new AggregateUpdate<>(true,
                Sequence.position(101L));

        byte[] serialised = serdes.aggregateUpdate().serializer().serialize(topic, update);
        AggregateUpdate<Boolean> deserialised = serdes.aggregateUpdate().deserializer().deserialize(topic, serialised);
        assertThat(deserialised).isEqualToComparingFieldByField(update);
    }

    @Test
    void commandRequest() {
        UserAccountId aggKey = new UserAccountId("userId");

        CommandRequest<UserAccountId, AccountEvent> commandRequest = CommandRequest.of(
                CommandId.random(),
                aggKey,
                Sequence.position(102L),
                new AccountEvent(new AccountCreated("name", BigDecimal.valueOf(1000, 4))));

        byte[] serialised = serdes.commandRequest().serializer().serialize(topic, commandRequest);
        CommandRequest<UserAccountId, AccountEvent> deserialised = serdes.commandRequest().deserializer().deserialize(topic, serialised);
        assertThat(deserialised).isEqualToComparingFieldByField(commandRequest);
    }

    @Test
    void eventWithSequence() {
        ValueWithSequence<AccountEvent> eventSeq = new ValueWithSequence<>(
                new AccountEvent(new AccountCreated("name", BigDecimal.valueOf(1000, 4))),
                Sequence.position(103L));

        byte[] serialised = serdes.valueWithSequence().serializer().serialize(topic, eventSeq);
        ValueWithSequence<AccountEvent> deserialised = serdes.valueWithSequence().deserializer().deserialize(topic, serialised);
        assertThat(deserialised).isEqualToComparingFieldByField(eventSeq);
    }

    @Test
    void commandResponseSuccess() {
        UserAccountId aggKey = new UserAccountId("userId");
        CommandResponse<UserAccountId> commandResponse = CommandResponse.of(
                CommandId.random(),
                aggKey,
                Sequence.position(104L),
                Result.success(Sequence.position(105L)));

        byte[] serialised = serdes.commandResponse().serializer().serialize(topic, commandResponse);
        CommandResponse<UserAccountId> deserialised = serdes.commandResponse().deserializer().deserialize(topic, serialised);
        assertThat(deserialised).isEqualToComparingFieldByField(commandResponse);
    }

    @Test
    void commandResponseFailure() {
        UserAccountId aggKey = new UserAccountId("userId");
        CommandResponse<UserAccountId> commandResponse = CommandResponse.of(
                CommandId.random(),
                aggKey,
                Sequence.position(106L),
                Result.failure(CommandError.of(CommandError.Reason.InvalidReadSequence, "Invalid sequence")));

        byte[] serialised = serdes.commandResponse().serializer().serialize(topic, commandResponse);
        CommandResponse<UserAccountId> deserialised = serdes.commandResponse().deserializer().deserialize(topic, serialised);
        assertThat(deserialised).isEqualToComparingFieldByField(commandResponse);
    }
}