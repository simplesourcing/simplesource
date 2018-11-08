package io.simplesource.kafka.serialization.avro.mappers;

import io.simplesource.kafka.serialization.avro.generated.*;
import io.simplesource.kafka.serialization.avro.mappers.domain.*;
import io.simplesource.kafka.serialization.util.GenericMapper;
import org.apache.avro.generic.GenericRecord;

import java.util.Optional;

import static io.simplesource.kafka.serialization.avro.AvroSpecificGenericMapper.specificDomainMapper;

public class UserAccountAvroMappers {
    public static final GenericMapper<Optional<UserAccountDomain>, GenericRecord> aggregateMapper = new GenericMapper<Optional<UserAccountDomain>, GenericRecord>() {
        @Override
        public GenericRecord toGeneric(final Optional<UserAccountDomain> maybeUserAccount) {
            return maybeUserAccount.map(user ->
                    io.simplesource.kafka.serialization.avro.generated.UserAccount.newBuilder()
                            .setName(user.userName())
                            .setBalance(user.balance().getAmount())
                            .build()
            ).orElse(null);
        }

        @Override
        public Optional<UserAccountDomain> fromGeneric(final GenericRecord serialized) {
            if (serialized == null) return Optional.empty();

            final GenericMapper<io.simplesource.kafka.serialization.avro.generated.UserAccount, GenericRecord> mapper = specificDomainMapper();
            final io.simplesource.kafka.serialization.avro.generated.UserAccount user = mapper.fromGeneric(serialized);
            return Optional.of(new UserAccountDomain(
                    user.getName(),
                    Money.valueOf(user.getBalance())));
        }
    };

    public static final GenericMapper<UserAccountDomainEvent, GenericRecord> eventMapper = new GenericMapper<UserAccountDomainEvent, GenericRecord>() {

        @Override
        public GenericRecord toGeneric(final UserAccountDomainEvent value) {
            if (value instanceof UserAccountDomainEvent.AccountCreated) {
                final UserAccountDomainEvent.AccountCreated event = (UserAccountDomainEvent.AccountCreated)value;
                return new AccountCreated(event.name(), event.balance().getAmount());
            }
            if (value instanceof UserAccountDomainEvent.UserNameUpdated) {
                final UserAccountDomainEvent.UserNameUpdated event = (UserAccountDomainEvent.UserNameUpdated)value;
                return new UserNameUpdated(event.name());
            }
            if (value instanceof UserAccountDomainEvent.AccountDeleted) {
                return new AccountDeleted();
            }
            if (value instanceof UserAccountDomainEvent.BuggyEvent) {
                return new BuggyEvent();
            }

            throw new IllegalArgumentException("Unknown UserAccountEvent " + value);
        }

        @Override
        public UserAccountDomainEvent fromGeneric(final GenericRecord serialized) {
            final GenericMapper<GenericRecord, GenericRecord> mapper = specificDomainMapper();
            final GenericRecord specificRecord = mapper.fromGeneric(serialized);
            if (specificRecord instanceof AccountCreated) {
                final AccountCreated event = (AccountCreated)specificRecord;
                return new UserAccountDomainEvent.AccountCreated(event.getName(), Money.valueOf(event.getBalance()));
            }
            if (specificRecord instanceof UserNameUpdated) {
                final UserNameUpdated event = (UserNameUpdated)specificRecord;
                return new UserAccountDomainEvent.UserNameUpdated(event.getName());
            }
            if (specificRecord instanceof AccountDeleted) {
                return new UserAccountDomainEvent.AccountDeleted();
            }
            if (specificRecord instanceof BuggyEvent) {
                return new UserAccountDomainEvent.BuggyEvent();
            }


            throw new IllegalArgumentException("Unknown UserAccountEvent " + serialized);
        }
    };

    public static final GenericMapper<UserAccountDomainCommand, GenericRecord> commandMapper = new GenericMapper<UserAccountDomainCommand, GenericRecord>() {

        @Override
        public GenericRecord toGeneric(final UserAccountDomainCommand value) {
            if (value instanceof UserAccountDomainCommand.CreateAccount) {
                final UserAccountDomainCommand.CreateAccount command = (UserAccountDomainCommand.CreateAccount)value;
                return new CreateAccount(command.name(), command.balance().getAmount());
            }
            if (value instanceof UserAccountDomainCommand.UpdateUserName) {
                final UserAccountDomainCommand.UpdateUserName command = (UserAccountDomainCommand.UpdateUserName)value;
                return new UpdateUserName(command.name());
            }
            if (value instanceof UserAccountDomainCommand.DeleteAccount) {
                return new DeleteAccount();
            }
            if (value instanceof UserAccountDomainCommand.BuggyCommand) {
                return new BuggyCommand();
            }

            throw new IllegalArgumentException("Unknown UserAccountCommand " + value);
        }

        @Override
        public UserAccountDomainCommand fromGeneric(final GenericRecord serialized) {
            final GenericMapper<GenericRecord, GenericRecord> mapper = specificDomainMapper();
            final GenericRecord specificRecord = mapper.fromGeneric(serialized);
            if (specificRecord instanceof CreateAccount) {
                final CreateAccount command = (CreateAccount)specificRecord;
                return new UserAccountDomainCommand.CreateAccount(command.getName(), Money.valueOf(command.getBalance()));
            }
            if (specificRecord instanceof UpdateUserName) {
                final UpdateUserName command = (UpdateUserName)specificRecord;
                return new UserAccountDomainCommand.UpdateUserName(command.getName());
            }
            if (specificRecord instanceof DeleteAccount) {
                return new UserAccountDomainCommand.DeleteAccount();
            }
            if (specificRecord instanceof BuggyCommand) {
                return new UserAccountDomainCommand.BuggyCommand();
            }

            throw new IllegalArgumentException("Unknown UserAccountEvent " + serialized);
        }
    };

    public static final GenericMapper<UserAccountDomainKey, GenericRecord> keyMapper = new GenericMapper<UserAccountDomainKey, GenericRecord>() {
        @Override
        public GenericRecord toGeneric(final UserAccountDomainKey value) {
            return UserAccountId.newBuilder()
                    .setId(value.userId())
                    .build();
        }

        @Override
        public UserAccountDomainKey fromGeneric(final GenericRecord serialized) {
            final GenericMapper<UserAccountId, GenericRecord> mapper = specificDomainMapper();
            final UserAccountId userId = mapper.fromGeneric(serialized);
            return new UserAccountDomainKey(userId.getId());
        }
    };
}
