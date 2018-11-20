package io.simplesource.testutils.avro;

import io.simplesource.api.CommandError;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Sequence;
import io.simplesource.kafka.api.AggregateSerdes;
import io.simplesource.kafka.testutils.AggregateTestDriver;
import io.simplesource.kafka.testutils.AggregateTestHelper;
import io.simplesource.testutils.domain.UserAggregate;
import io.simplesource.testutils.domain.User;
import io.simplesource.testutils.domain.UserCommand;
import io.simplesource.testutils.domain.UserEvent;
import io.simplesource.testutils.domain.UserKey;
import io.simplesource.kafka.dsl.KafkaConfig;
import io.simplesource.kafka.util.PrefixResourceNamingStrategy;
import io.simplesource.kafka.model.ValueWithSequence;
import io.simplesource.kafka.serialization.avro.AvroAggregateSerdes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.simplesource.testutils.domain.UserAvroMappers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class UserAvroKStreamTest {
    private AggregateTestDriver<UserKey, UserCommand, UserEvent, Optional<User>> testAPI;
    private AggregateTestHelper<UserKey, UserCommand, UserEvent, Optional<User>> testHelper;

    @BeforeEach
    public void setup() {
        final AggregateSerdes<UserKey, UserCommand, UserEvent, Optional<User>> avroAggregateSerdes =
                new AvroAggregateSerdes<>(
                        keyMapper, commandMapper, eventMapper, aggregateMapper,
                        "http://mock-registry:8081",
                        true,
                        io.simplesource.testutils.generated.User.SCHEMA$);

        testAPI = new AggregateTestDriver<>(
                UserAggregate.createSpec(
                        "user",
                        avroAggregateSerdes,
                        new PrefixResourceNamingStrategy("user_mapped_avro_"),
                        k -> Optional.empty()
                ),
                new KafkaConfig.Builder()
                        .withKafkaApplicationId("testApp")
                        .withKafkaBootstrap("0.0.0.0:9092")
                        .withExactlyOnce()
                        .build());
        testHelper = new AggregateTestHelper<>(testAPI);
    }

    @AfterEach
    public void tearDown() {
        if (testAPI != null) {
            testAPI.close();
        }
    }

    @Test
    public void standardUserWorkflow() {
        final UserKey key = new UserKey("user2345");
        final String firstName = "Bob";
        final String lastName = "Dubois";
        final String updatedFirstName = "Bobbette";
        final String updatedLastName = "Dubois III";
        final int yearOfBirth = 1991;

        testHelper.publishCommand(
                key,
                Sequence.first(),
                new UserCommand.InsertUser(firstName, lastName))
                .expecting(
                        NonEmptyList.of(new UserEvent.UserInserted(firstName, lastName)),
                        Optional.of(new User(firstName, lastName, null))
                )
                .thenPublish(
                        new UserCommand.UpdateName(updatedFirstName, updatedLastName))
                .expecting(
                        NonEmptyList.of(
                                new UserEvent.FirstNameUpdated(updatedFirstName),
                                new UserEvent.LastNameUpdated(updatedLastName)),
                        Optional.of(new User(updatedFirstName, updatedLastName, null))
                )
                .thenPublish(
                        new UserCommand.UpdateYearOfBirth(yearOfBirth))
                .expecting(
                        NonEmptyList.of(new UserEvent.YearOfBirthUpdated(yearOfBirth)),
                        Optional.of(new User(updatedFirstName, updatedLastName, yearOfBirth))
                )
                .thenPublish(
                        new UserCommand.DeleteUser())
                .expecting(
                        NonEmptyList.of(new UserEvent.UserDeleted()),
                        Optional.empty()
                );

    }

    @Test
    public void updateBeforeInsert() {
        final UserKey id = new UserKey("national1");
        final String firstName = "Barnady";
        final String lastName = "Joyce";

        testHelper.publishCommand(
                id,
                Sequence.first(),
                new UserCommand.UpdateName(firstName, lastName))
                .expectingFailure(NonEmptyList.of(CommandError.Reason.InvalidCommand));
    }

    @Test
    public void invalidSequenceOnInsert() {
        final UserKey id = new UserKey("national2");
        final String firstName = "Michael";
        final String lastName = "McCormack";

        testHelper.publishCommand(
                id,
                Sequence.position(666L),
                new UserCommand.InsertUser(firstName, lastName))
                .expectingFailure(NonEmptyList.of(CommandError.Reason.InvalidReadSequence));
    }

    @Test
    public void invalidSequenceIdOnUpdate() {
        final UserKey id = new UserKey("myuser");
        final String firstName = "Renee";
        final String lastName = "Renet";
        final String updatedFirstName = "Renette";
        final String updatedLastName = "Rented";

        testHelper.publishCommand(
                id,
                Sequence.first(),
                new UserCommand.InsertUser(firstName, lastName))
                .expecting(
                        NonEmptyList.of(new UserEvent.UserInserted(firstName, lastName)),
                        Optional.of(new User(firstName, lastName, null))
                )
                .thenPublish(update ->
                        new ValueWithSequence<>(new UserCommand.UpdateName(updatedFirstName, updatedLastName), Sequence.first()))
                .expectingFailure(NonEmptyList.of(CommandError.Reason.InvalidReadSequence));
    }

    @Test
    public void invalidCommand() {
        final UserKey id = new UserKey("myuser");

        testHelper.publishCommand(
                id,
                Sequence.first(),
                new UserCommand.UnhandledCommand())
                .expectingFailure(NonEmptyList.of(CommandError.Reason.InvalidCommand));
    }

    @Test
    public void buggyCommandHandler() {
        final UserKey id = new UserKey("myuser");

        testHelper.publishCommand(
                id,
                Sequence.first(),
                new UserCommand.BuggyCommand(true, false))
                .expectingFailure(NonEmptyList.of(CommandError.Reason.CommandHandlerFailed));
    }

    @Test
    public void buggyEventHandler() {
        final UserKey id = new UserKey("myuser");

        assertThrows(org.opentest4j.AssertionFailedError.class, () ->
                testHelper.publishCommand(
                        id,
                        Sequence.first(),
                        new UserCommand.BuggyCommand(false, true))
                        .expectingFailure(NonEmptyList.of(CommandError.Reason.CommandPublishError))
        );
    }

}
