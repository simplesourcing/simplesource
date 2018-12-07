package io.simplesource.dsl;

import io.simplesource.api.CommandHandler;
import io.simplesource.api.CommandError;
import io.simplesource.api.CommandError.Reason;
import io.simplesource.data.Result;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * An builder for creating an {@link CommandHandler} that can handle several different commands types by adding
 * one or more single command handlers.
 *
 * @param <K> the aggregate key type
 * @param <C> all commands for this aggregate
 * @param <E> all events generated for this aggregate
 * @param <A> the aggregate type
 */
public final class CommandHandlerBuilder<K, C, E, A> {

    public static <K, C, E, A> CommandHandlerBuilder<K, C, E, A> newBuilder() {
        return new CommandHandlerBuilder<>();
    }

    private final Map<Class<? extends C>, CommandHandler<K, ? extends C, E, A>> commandHandlers = new HashMap<>();

    private CommandHandlerBuilder() {}

    public <SC extends C> CommandHandlerBuilder<K, C, E, A> onCommand(final Class<SC> specificCommandClass, final CommandHandler<K, SC, E, A> ch) {
        commandHandlers.put(specificCommandClass, ch);
        return this;
    }

    private Map<Class<? extends C>, CommandHandler<K, ? extends C, E, A>> getCommandHandlers() {
        return new HashMap<>(commandHandlers);
    }

    public <SC extends C> CommandHandler<K, SC, E, A> build() {
        // defensive copy
        final Map<Class<? extends C>, CommandHandler<K, ? extends C, E, A>> ch = getCommandHandlers();

        return (key, currentAggregate, command) -> {
            final CommandHandler<K, SC, E, A> commandHandler = (CommandHandler<K, SC, E, A>) ch.get(command.getClass());
            if (commandHandler == null) {
                return Result.failure(CommandError.of(Reason.InvalidCommand, String.format("Unhandled command type: %s",
                        command.getClass().getSimpleName())));
            }

            return commandHandler.interpretCommand(key, currentAggregate, command);
        };
    }
}
