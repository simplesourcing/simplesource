package io.simplesource.kafka.serialization.avro.mappers;

import io.simplesource.api.CommandError;
import io.simplesource.api.CommandId;
import io.simplesource.data.Result;
import io.simplesource.data.Sequence;
import io.simplesource.kafka.api.CommandSerdes;
import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.kafka.model.CommandResponse;
import io.simplesource.kafka.serialization.avro.AvroSerdes;
import io.simplesource.kafka.serialization.avro.mappers.domain.UserAccountDomainCommand;
import io.simplesource.kafka.serialization.avro.mappers.domain.UserAccountDomainKey;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


import static org.assertj.core.api.Assertions.assertThat;

class AvroCustomCommandSerdeTests {
    private static final String topic = "topic";
    private CommandSerdes<UserAccountDomainKey, UserAccountDomainCommand> serdes;

    @BeforeEach
    void setup() {
        serdes = AvroSerdes.Custom.command(
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
        CommandId responseKey = CommandId.random();

        byte[] serialised = serdes.commandId().serializer().serialize(topic, responseKey);
        CommandId deserialised = serdes.commandId().deserializer().deserialize(topic, serialised);
        assertThat(deserialised).isEqualToComparingFieldByField(responseKey);
    }

    @Test
    void commandRequest() {
        UserAccountDomainKey aggKey = new UserAccountDomainKey("userId");

        CommandRequest<UserAccountDomainKey, UserAccountDomainCommand> commandRequest = CommandRequest.of(
                CommandId.random(),
                aggKey,
                Sequence.first(),
                new UserAccountDomainCommand.UpdateUserName("name"));

        byte[] serialised = serdes.commandRequest().serializer().serialize(topic, commandRequest);
        CommandRequest<UserAccountDomainKey, UserAccountDomainCommand> deserialised = serdes.commandRequest().deserializer().deserialize(topic, serialised);
        assertThat(deserialised).isEqualToComparingFieldByField(commandRequest);
    }

    @Test
    void commandResponseSuccess() {
        UserAccountDomainKey aggKey = new UserAccountDomainKey("userId");

        CommandResponse commandResponse = CommandResponse.of(
                CommandId.random(),
                aggKey,
                Sequence.first(),
                Result.success(Sequence.first()));

        byte[] serialised = serdes.commandResponse().serializer().serialize(topic, commandResponse);
        CommandResponse deserialised = serdes.commandResponse().deserializer().deserialize(topic, serialised);
        assertThat(deserialised).isEqualToComparingFieldByField(commandResponse);
    }

    @Test
    void commandResponseFailure() {
        UserAccountDomainKey aggKey = new UserAccountDomainKey("userId");

        CommandResponse commandResponse = CommandResponse.of(
                CommandId.random(),
                aggKey,
                Sequence.first(),
                Result.failure(CommandError.of(CommandError.Reason.InvalidReadSequence, "Invalid sequence")));

        byte[] serialised = serdes.commandResponse().serializer().serialize(topic, commandResponse);
        CommandResponse deserialised = serdes.commandResponse().deserializer().deserialize(topic, serialised);
        assertThat(deserialised).isEqualToComparingFieldByField(commandResponse);
    }
}