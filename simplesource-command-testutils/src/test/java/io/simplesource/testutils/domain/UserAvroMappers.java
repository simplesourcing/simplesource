package io.simplesource.testutils.domain;

import io.simplesource.testutils.generated.*;
import io.simplesource.kafka.serialization.util.GenericMapper;
import org.apache.avro.generic.GenericRecord;

import java.util.Optional;

import static io.simplesource.kafka.serialization.avro.AvroSpecificGenericMapper.specificDomainMapper;

public final class UserAvroMappers {

    public static final GenericMapper<Optional<User>, GenericRecord> aggregateMapper = new GenericMapper<Optional<User>, GenericRecord>() {
        @Override
        public GenericRecord toGeneric(final Optional<User> maybeUser) {
            return maybeUser.map(user ->
                io.simplesource.testutils.generated.User.newBuilder()
                        .setFirstName(user.firstName())
                        .setLastName(user.lastName())
                        .setYearOfBirth(user.yearOfBirth())
                        .build()
            ).orElse(null);
        }

        @Override
        public Optional<User> fromGeneric(final GenericRecord serialized) {
            if (serialized == null) return Optional.empty();

            final GenericMapper<io.simplesource.testutils.generated.User, GenericRecord> mapper = specificDomainMapper();
            final io.simplesource.testutils.generated.User user = mapper.fromGeneric(serialized);
            return Optional.of(new User(
                //new UserKey(user.getUserId().getId()),
                user.getFirstName(),
                user.getLastName(),
                user.getYearOfBirth()));
        }
    };

    public static final GenericMapper<UserEvent, GenericRecord> eventMapper = new GenericMapper<UserEvent, GenericRecord>() {

        @Override
        public GenericRecord toGeneric(final UserEvent value) {
            if (value instanceof UserEvent.UserInserted) {
                final UserEvent.UserInserted event = (UserEvent.UserInserted)value;
                return new UserInserted(event.firstName(), event.lastName());
            }
            if (value instanceof UserEvent.FirstNameUpdated) {
                final UserEvent.FirstNameUpdated event = (UserEvent.FirstNameUpdated)value;
                return new FirstNameUpdated(event.firstName());
            }
            if (value instanceof UserEvent.LastNameUpdated) {
                final UserEvent.LastNameUpdated event = (UserEvent.LastNameUpdated)value;
                return new LastNameUpdated(event.lastName());
            }
            if (value instanceof UserEvent.YearOfBirthUpdated) {
                final UserEvent.YearOfBirthUpdated event = (UserEvent.YearOfBirthUpdated)value;
                return new YearOfBirthUpdated(event.yearOfBirth());
            }
            if (value instanceof UserEvent.UserDeleted) {
                return new UserDeleted();
            }
            if (value instanceof UserEvent.BuggyEvent) {
                return new BuggyEvent();
            }

            throw new IllegalArgumentException("Unknown UserEvent " + value);
        }

        @Override
        public UserEvent fromGeneric(final GenericRecord serialized) {
            final GenericMapper<GenericRecord, GenericRecord> mapper = specificDomainMapper();
            final GenericRecord specificRecord = mapper.fromGeneric(serialized);
            if (specificRecord instanceof UserInserted) {
                final UserInserted event = (UserInserted)specificRecord;
                return new UserEvent.UserInserted(event.getFirstName(), event.getLastName());
            }
            if (specificRecord instanceof FirstNameUpdated) {
                final FirstNameUpdated event = (FirstNameUpdated)specificRecord;
                return new UserEvent.FirstNameUpdated(event.getFirstName());
            }
            if (specificRecord instanceof LastNameUpdated) {
                final LastNameUpdated event = (LastNameUpdated)specificRecord;
                return new UserEvent.LastNameUpdated(event.getLastName());
            }
            if (specificRecord instanceof YearOfBirthUpdated) {
                final YearOfBirthUpdated event = (YearOfBirthUpdated)specificRecord;
                return new UserEvent.YearOfBirthUpdated(event.getYearOfBirth());
            }
            if (specificRecord instanceof UserDeleted) {
                return new UserEvent.UserDeleted();
            }
            if (specificRecord instanceof BuggyEvent) {
                return new UserEvent.BuggyEvent();
            }

            throw new IllegalArgumentException("Unknown UserEvent " + serialized);
        }
    };

    public static final GenericMapper<UserCommand, GenericRecord> commandMapper = new GenericMapper<UserCommand, GenericRecord>() {

        @Override
        public GenericRecord toGeneric(final UserCommand value) {
            if (value instanceof UserCommand.InsertUser) {
                final UserCommand.InsertUser command = (UserCommand.InsertUser)value;
                return new InsertUser(command.firstName(), command.lastName());
            }
            if (value instanceof UserCommand.UpdateName) {
                final UserCommand.UpdateName command = (UserCommand.UpdateName)value;
                return new UpdateName(command.firstName(), command.lastName());
            }
            if (value instanceof UserCommand.UpdateYearOfBirth) {
                final UserCommand.UpdateYearOfBirth command = (UserCommand.UpdateYearOfBirth)value;
                return new UpdateYearOfBirth(command.yearOfBirth());
            }
            if (value instanceof UserCommand.DeleteUser) {
                return new DeleteUser();
            }
            if (value instanceof UserCommand.BuggyCommand) {
                final UserCommand.BuggyCommand command = (UserCommand.BuggyCommand)value;
                return new BuggyCommand(command.throwInCommandHandler(), command.throwInEventHandler());
            }
            if (value instanceof UserCommand.UnhandledCommand) {
                return new UnhandledCommand();
            }

            throw new IllegalArgumentException("Unknown UserCommand " + value);
        }

        @Override
        public UserCommand fromGeneric(final GenericRecord serialized) {
            final GenericMapper<GenericRecord, GenericRecord> mapper = specificDomainMapper();
            final GenericRecord specificRecord = mapper.fromGeneric(serialized);
            if (specificRecord instanceof InsertUser) {
                final InsertUser command = (InsertUser)specificRecord;
                return new UserCommand.InsertUser(command.getFirstName(), command.getLastName());
            }
            if (specificRecord instanceof UpdateName) {
                final UpdateName command = (UpdateName)specificRecord;
                return new UserCommand.UpdateName(command.getFirstName(), command.getLastName());
            }
            if (specificRecord instanceof UpdateYearOfBirth) {
                final UpdateYearOfBirth command = (UpdateYearOfBirth)specificRecord;
                return new UserCommand.UpdateYearOfBirth(command.getYearOfBirth());
            }
            if (specificRecord instanceof DeleteUser) {
                return new UserCommand.DeleteUser();
            }
            if (specificRecord instanceof BuggyCommand) {
                final BuggyCommand command = (BuggyCommand)specificRecord;
                return new UserCommand.BuggyCommand(command.getThrowInCommandHandler(), command.getThrowInEventHandler());
            }
            if (specificRecord instanceof UnhandledCommand) {
                return new UserCommand.UnhandledCommand();
            }

            throw new IllegalArgumentException("Unknown UserEvent " + serialized);
        }
    };

    public static final GenericMapper<UserKey, GenericRecord> keyMapper = new GenericMapper<UserKey, GenericRecord>() {
        @Override
        public GenericRecord toGeneric(final UserKey value) {
            return UserId.newBuilder()
                .setId(value.id())
                .build();
        }

        @Override
        public UserKey fromGeneric(final GenericRecord serialized) {
            final GenericMapper<UserId, GenericRecord> mapper = specificDomainMapper();
            final UserId userId = mapper.fromGeneric(serialized);
            return new UserKey(userId.getId());
        }
    };

}
