package io.simplesource.kafka.serialization.avro.mappers;

import io.simplesource.api.CommandError;
import io.simplesource.data.Result;
import io.simplesource.data.Sequence;
import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.kafka.model.CommandResponse;
import io.simplesource.kafka.serialization.avro.AvroCommandSerdes;
import io.simplesource.kafka.serialization.avro.mappers.domain.UserAccountDomainCommand;
import io.simplesource.kafka.serialization.avro.mappers.domain.UserAccountDomainKey;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class AvroCommandSerdeTests {
    private static final String topic = "topic";
    private AvroCommandSerdes<UserAccountDomainKey, UserAccountDomainCommand> serdes;

    @BeforeEach
    void setup() {
        serdes = new AvroCommandSerdes<>(
                UserAccountAvroMappers.keyMapper,
                UserAccountAvroMappers.commandMapper,
                "http://localhost:8081",
                true);
    }

    @Test
    void aggregateKey() {
        UserAccountDomainKey aggKey = new UserAccountDomainKey("userId");
        byte[] serialised = serdes.aggregateKey().serializer().serialize(topic, aggKey);
        UserAccountDomainKey deserialised = serdes.aggregateKey().deserializer().deserialize(topic, serialised);
        assertThat(deserialised).isEqualToComparingFieldByField(aggKey);
    }

    @Test
    void uuidResponseKey() {
        UUID responseKey = UUID.randomUUID();

        byte[] serialised = serdes.commandResponseKey().serializer().serialize(topic, responseKey);
        UUID deserialised = serdes.commandResponseKey().deserializer().deserialize(topic, serialised);
        assertThat(deserialised).isEqualTo(responseKey);
    }

    @Test
    void commandRequest() {
        UserAccountDomainKey aggKey = new UserAccountDomainKey("userId");

        CommandRequest<UserAccountDomainKey, UserAccountDomainCommand> commandRequest = new CommandRequest<>(
                aggKey,
                new UserAccountDomainCommand.UpdateUserName("name"),
                Sequence.first(),
                UUID.randomUUID());

        byte[] serialised = serdes.commandRequest().serializer().serialize(topic, commandRequest);
        CommandRequest<UserAccountDomainKey, UserAccountDomainCommand> deserialised = serdes.commandRequest().deserializer().deserialize(topic, serialised);
        assertThat(deserialised).isEqualToComparingFieldByField(commandRequest);
    }

    @Test
    void commandResponseSuccess() {
        CommandResponse commandResponse = new CommandResponse(
                UUID.randomUUID(),
                Sequence.first(),
                Result.success(Sequence.first()));

        byte[] serialised = serdes.commandResponse().serializer().serialize(topic, commandResponse);
        CommandResponse deserialised = serdes.commandResponse().deserializer().deserialize(topic, serialised);
        assertThat(deserialised).isEqualToComparingFieldByField(commandResponse);
    }

    @Test
    void commandResponseFailure() {
        CommandResponse commandResponse = new CommandResponse(
                UUID.randomUUID(),
                Sequence.first(),
                Result.failure(CommandError.of(CommandError.Reason.InvalidReadSequence, "Invalid sequence")));

        byte[] serialised = serdes.commandResponse().serializer().serialize(topic, commandResponse);
        CommandResponse deserialised = serdes.commandResponse().deserializer().deserialize(topic, serialised);
        assertThat(deserialised).isEqualToComparingFieldByField(commandResponse);
    }
}