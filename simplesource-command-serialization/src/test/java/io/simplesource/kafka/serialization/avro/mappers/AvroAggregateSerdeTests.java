package io.simplesource.kafka.serialization.avro.mappers;

import io.simplesource.api.CommandError;
import io.simplesource.api.CommandId;
import io.simplesource.data.Result;
import io.simplesource.data.Sequence;
import io.simplesource.kafka.api.AggregateSerdes;
import io.simplesource.kafka.model.*;
import io.simplesource.kafka.serialization.avro.AvroAggregateSerdes;
import io.simplesource.kafka.serialization.avro.mappers.domain.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class AvroAggregateSerdeTests {
    private static final String topic = "topic";
    private AggregateSerdes<UserAccountDomainKey, UserAccountDomainCommand, UserAccountDomainEvent, Optional<UserAccountDomain>> serdes;

    @BeforeEach
    void setup() {
        serdes = new AvroAggregateSerdes<>(
                UserAccountAvroMappers.keyMapper,
                UserAccountAvroMappers.commandMapper,
                UserAccountAvroMappers.eventMapper,
                UserAccountAvroMappers.aggregateMapper,
                "http://localhost:8081",
                true,
                io.simplesource.kafka.serialization.avro.generated.UserAccount.SCHEMA$);
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

        byte[] serialised = serdes.commandResponseKey().serializer().serialize(topic, responseKey);
        CommandId deserialised = serdes.commandResponseKey().deserializer().deserialize(topic, serialised);
        assertThat(deserialised).isEqualTo(responseKey);
    }

    @Test
    void aggregateUpdate() {
        AggregateUpdate<Optional<UserAccountDomain>> update = new AggregateUpdate<>(
                Optional.of(new UserAccountDomain("Name", Money.valueOf("100"))),
                Sequence.first());

        byte[] serialised = serdes.aggregateUpdate().serializer().serialize(topic, update);
        AggregateUpdate<Optional<UserAccountDomain>> deserialised = serdes.aggregateUpdate().deserializer().deserialize(topic, serialised);
        assertThat(deserialised).isEqualToComparingFieldByField(update);
    }

    @Test
    void commandRequest() {
        UserAccountDomainKey aggKey = new UserAccountDomainKey("userId");

        CommandRequest<UserAccountDomainKey, UserAccountDomainCommand> commandRequest = new CommandRequest<>(
                CommandId.random(),
                aggKey,
                Sequence.first(),
                new UserAccountDomainCommand.UpdateUserName("name"));

        byte[] serialised = serdes.commandRequest().serializer().serialize(topic, commandRequest);
        CommandRequest<UserAccountDomainKey, UserAccountDomainCommand> deserialised = serdes.commandRequest().deserializer().deserialize(topic, serialised);
        assertThat(deserialised).isEqualToComparingFieldByField(commandRequest);
    }

    @Test
    void eventWithSequence() {
        ValueWithSequence<UserAccountDomainEvent> eventSeq = new ValueWithSequence<>(
                new UserAccountDomainEvent.AccountCreated("name", Money.valueOf("100")),
                Sequence.first()                );

        byte[] serialised = serdes.valueWithSequence().serializer().serialize(topic, eventSeq);
        ValueWithSequence<UserAccountDomainEvent> deserialised = serdes.valueWithSequence().deserializer().deserialize(topic, serialised);
        assertThat(deserialised).isEqualToComparingFieldByField(eventSeq);
    }

    @Test
    void commandResponseSuccess() {
        UserAccountDomainKey aggKey = new UserAccountDomainKey("userId");
        CommandResponse commandResponse = new CommandResponse(
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
        CommandResponse commandResponse = new CommandResponse(
                CommandId.random(),
                aggKey,
                Sequence.first(),
                Result.failure(CommandError.of(CommandError.Reason.InvalidReadSequence, "Invalid sequence")));

        byte[] serialised = serdes.commandResponse().serializer().serialize(topic, commandResponse);
        CommandResponse deserialised = serdes.commandResponse().deserializer().deserialize(topic, serialised);
        assertThat(deserialised).isEqualToComparingFieldByField(commandResponse);
    }

}
