package io.simplesource.dsl;


import io.simplesource.api.*;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

import java.util.Optional;

import static io.simplesource.data.NonEmptyList.of;
import static java.util.Optional.empty;

@Value
@Builder(toBuilder = true)
@AllArgsConstructor
final class User {
    private final String firstName;
    private final String lastName;
    private final Integer yearOfBirth;
}

interface UserCommand {
    @Value
    final class UserKey {
        private final String id;
    }
    UserKey key();

    @Value
    class InsertUser implements UserCommand {
        private final UserKey key;
        private final String firstName;
        private final String lastName;
    }

    @Value
    class UpdateName implements UserCommand {
        private final UserKey key;
        private final String firstName;
        private final String lastName;
    }

    @Value
    class UpdateYearOfBirth implements UserCommand {
        private final UserKey key;
        private final Integer yearOfBirth;
    }

    @Value
    class DeleteUser implements UserCommand {
        private final UserKey key;
    }

    @Value
    class BuggyCommand implements UserCommand {
        private final UserKey key;
        private final boolean throwInCommandHandler;
        private final boolean throwInEventHandler;
    }

    @Value
    class UnhandledCommand implements UserCommand {
        private final UserKey key;
    }

    static CommandHandler<InsertUser, UserEvent, Optional<User>> doInsertUser() {
        return (currentAggregate, command) -> currentAggregate
                        .map(d -> failure("User already created: " + command.key.id()))
                        .orElse(success(new UserEvent.UserInserted(
                                command.firstName(),
                                command.lastName())));
    }

    static CommandHandler<DeleteUser, UserEvent, Optional<User>> doDeleteUser() {
        return (currentAggregate, command) -> currentAggregate
                        .map(d -> success(new UserEvent.UserDeleted()))
                        .orElse(failure("Attempted to delete non-existent user: " + command.key().id()));
    }

    static CommandHandler<UpdateYearOfBirth, UserEvent, Optional<User>> doUpdateYearOfBirth() {
        return (currentAggregate, command) -> currentAggregate
                        .map(d -> success(new UserEvent.YearOfBirthUpdated(command.yearOfBirth())))
                        .orElse(failure("Attempted to update non-existent user: " + command.key().id()));
    }

    static CommandHandler<UpdateName, UserEvent, Optional<User>> doUpdateName() {
        return (currentAggregate, command) -> currentAggregate
                        .map(d -> success(
                                new UserEvent.FirstNameUpdated(command.firstName()),
                                new UserEvent.LastNameUpdated(command.lastName())))
                        .orElse(failure("Attempted to update non-existent user: " + command.key().id()));
    }

    static CommandHandler<BuggyCommand, UserEvent, Optional<User>> doBuggyCommand() {
        return (currentAggregate, command) -> {
                    if (command.throwInCommandHandler()) {
                        throw new RuntimeException("Buggy bug");
                    } else {
                        return success(new UserEvent.BuggyEvent());
                    }
                };
    }

    static Result<CommandError, NonEmptyList<UserEvent>> failure(final String message) {
        return Result.failure(CommandError.of(CommandError.Reason.InvalidCommand, message));
    }

    @SafeVarargs
    static <Event extends UserEvent> Result<CommandError, NonEmptyList<UserEvent>> success(final Event event, final Event... events) {
        return Result.success(of(event, events));
    }

    static CommandHandler<UserCommand, UserEvent, Optional<User>> getCommandHandler() {
        return CommandHandlerBuilder.<UserKey, UserCommand, UserEvent, Optional<User>>newBuilder()
                // Command handling
                .onCommand(InsertUser.class, doInsertUser())
                .onCommand(UpdateName.class, doUpdateName())
                .onCommand(UpdateYearOfBirth.class, doUpdateYearOfBirth())
                .onCommand(DeleteUser.class, doDeleteUser())
                .onCommand(BuggyCommand.class, doBuggyCommand())
                .build();
    }

    static CommandAggregateKey<UserKey, UserCommand> getAggregateKey() {
        return c -> Result.success(c.key());
    }
}

interface UserEvent {

    @Value
    class UserInserted implements UserEvent {
        private final String firstName;
        private final String lastName;
    }

    @Value
    class FirstNameUpdated implements UserEvent {
        private final String firstName;
    }

    @Value
    class LastNameUpdated implements UserEvent {
        private final String lastName;
    }

    @Value
    class YearOfBirthUpdated implements UserEvent {
        private final Integer yearOfBirth;
    }

    @Value
    class UserDeleted implements UserEvent {
    }

    @Value
    class BuggyEvent implements UserEvent {
    }


    static Aggregator<BuggyEvent, Optional<User>> handleBuggyEvent() {
        return (currentAggregate, event) -> {
            throw new UnsupportedOperationException();
        };
    }

    static Aggregator<UserDeleted, Optional<User>> handleUserDeleted() {
        return (currentAggregate, event) -> empty();
    }

    static Aggregator<YearOfBirthUpdated, Optional<User>> handleYearOfBirthUpdated() {
        return (currentAggregate, event) ->
                currentAggregate.map(user -> user.toBuilder()
                        .yearOfBirth(event.yearOfBirth())
                        .build());
    }

    static Aggregator<LastNameUpdated, Optional<User>> handleLastNameUpdated() {
        return (currentAggregate, event) ->
                currentAggregate.map(user -> user.toBuilder()
                        .lastName(event.lastName())
                        .build());
    }

    static Aggregator<FirstNameUpdated, Optional<User>> handleFirstNameUpdated() {
        return (currentAggregate, event) ->
                currentAggregate.map(user -> user.toBuilder()
                        .firstName(event.firstName())
                        .build());
    }

    static Aggregator<UserInserted, Optional<User>> handleUserInserted() {
        return (currentAggregate, event) ->
                Optional.of(new User(event.firstName(), event.lastName(), null));
    }


    static Aggregator<UserEvent, Optional<User>> getAggregator() {
        return AggregatorBuilder.<UserEvent, Optional<User>> newBuilder()
                .onEvent(UserInserted.class, handleUserInserted())
                .onEvent(FirstNameUpdated.class, handleFirstNameUpdated())
                .onEvent(LastNameUpdated.class, handleLastNameUpdated())
                .onEvent(YearOfBirthUpdated.class, handleYearOfBirthUpdated())
                .onEvent(UserDeleted.class, handleUserDeleted())
                .onEvent(BuggyEvent.class, handleBuggyEvent())
                .build();
    }

}
