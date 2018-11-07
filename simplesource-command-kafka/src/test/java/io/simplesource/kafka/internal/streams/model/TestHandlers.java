package io.simplesource.kafka.internal.streams.model;

import io.simplesource.api.Aggregator;
import io.simplesource.api.CommandError;
import io.simplesource.api.CommandHandler;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;

import java.util.Optional;

public class TestHandlers {
    static public CommandHandler<String, TestCommand, TestEvent, Optional<TestAggregate>> commandHandler =
            (key, currentAggregate, command) -> {
                if (command instanceof TestCommand.CreateCommand) {
                    TestCommand.CreateCommand create = (TestCommand.CreateCommand) command;
                    if (currentAggregate.isPresent())
                        return Result.failure(CommandError.of(CommandError.Reason.InvalidCommand, "Already exists"));
                    return Result.success(NonEmptyList.of(new TestEvent.Created(create.name())));
                }

                if (command instanceof TestCommand.UpdateCommand) {
                    TestCommand.UpdateCommand update = (TestCommand.UpdateCommand) command;
                    if (!currentAggregate.isPresent())
                        return Result.failure(CommandError.of(CommandError.Reason.InvalidCommand, "Doesn't exist"));
                    return Result.success(NonEmptyList.of(new TestEvent.Updated(update.name())));
                }

                if (command instanceof TestCommand.UpdateWithNothingCommand) {
                    TestCommand.UpdateWithNothingCommand createCommand = (TestCommand.UpdateWithNothingCommand) command;
                    return Result.success(NonEmptyList.of(
                            new TestEvent.Updated(createCommand.name()),
                            new TestEvent.DoesNothing(),
                            new TestEvent.DoesNothing()));
                }

                return Result.failure(CommandError.of(CommandError.Reason.InvalidCommand, "Command not supported"));
            };

    static public Aggregator<TestEvent, Optional<TestAggregate>> eventAggregator =
            (currentAggregate, event) -> {
                if (event instanceof TestEvent.Created) {
                    TestEvent.Created createEvent = (TestEvent.Created) event;
                    return Optional.of(new TestAggregate(createEvent.name()));
                }
                if (event instanceof TestEvent.Updated) {
                    TestEvent.Updated updateEvent = (TestEvent.Updated) event;
                    return Optional.of(new TestAggregate(updateEvent.name()));
                }
                return currentAggregate;
            };

}
