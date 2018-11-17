package io.simplesource.testutils.domain;

import io.simplesource.api.CommandError;
import io.simplesource.api.CommandHandler;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.dsl.CommandHandlerBuilder;
import lombok.Value;

import java.util.Optional;

import static io.simplesource.data.NonEmptyList.of;

public interface UserCommand {

    @Value
    class InsertUser implements UserCommand {
        private final String firstName;
        private final String lastName;
    }

    @Value
    class UpdateName implements UserCommand {
        private final String firstName;
        private final String lastName;
    }

    @Value
    class UpdateYearOfBirth implements UserCommand {
        private final Integer yearOfBirth;
    }

    @Value
    class DeleteUser implements UserCommand {
    }

    @Value
    class BuggyCommand implements UserCommand {
        private final boolean throwInCommandHandler;
        private final boolean throwInEventHandler;
    }

    @Value
    class UnhandledCommand implements UserCommand {
    }

    static CommandHandler<UserKey, InsertUser, UserEvent, Optional<User>> doInsertUser() {
        return (userId, currentAggregate, command) -> currentAggregate
                .map(d -> failure("User already created: " + userId.id()))
                .orElse(success(new UserEvent.UserInserted(
                        command.firstName(),
                        command.lastName())));
    }

    static CommandHandler<UserKey, DeleteUser, UserEvent, Optional<User>> doDeleteUser() {
        return (userId, currentAggregate, command) -> currentAggregate
                .map(d -> success(new UserEvent.UserDeleted()))
                .orElse(failure("Attempted to delete non-existent user: " + userId.id()));
    }

    static CommandHandler<UserKey, UpdateYearOfBirth, UserEvent, Optional<User>> doUpdateYearOfBirth() {
        return (userId, currentAggregate, command) -> currentAggregate
                .map(d -> success(
                        new UserEvent.YearOfBirthUpdated(command.yearOfBirth())))
                .orElse(failure("Attempted to update non-existent user: " + userId.id()));
    }

    static CommandHandler<UserKey, UpdateName, UserEvent, Optional<User>> doUpdateName() {
        return (userId, currentAggregate, command) -> currentAggregate
                .map(d -> success(
                        new UserEvent.FirstNameUpdated(command.firstName()),
                        new UserEvent.LastNameUpdated(command.lastName())))
                .orElse(failure("Attempted to update non-existent user: " + userId.id()));
    }

    static CommandHandler<UserKey, BuggyCommand, UserEvent, Optional<User>> doBuggyCommand() {
        return (userId, currentAggregate, command) -> {
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

    static CommandHandler<UserKey, UserCommand, UserEvent, Optional<User>> getCommandHandler() {
        return CommandHandlerBuilder.<UserKey, UserCommand, UserEvent, Optional<User>>newBuilder()
                // Command handling
                .onCommand(InsertUser.class, doInsertUser())
                .onCommand(UpdateName.class, doUpdateName())
                .onCommand(UpdateYearOfBirth.class, doUpdateYearOfBirth())
                .onCommand(DeleteUser.class, doDeleteUser())
                .onCommand(BuggyCommand.class, doBuggyCommand())
                .build();
    }

}
