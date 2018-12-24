package io.simplesource.api;

import io.simplesource.data.Error;
import lombok.Value;

/**
 * A CommandError explains failures. They can be constructed from the reason type,
 * with either a {@link String} or a {@link Throwable} for more context.
 */
@Value
public class CommandError {
    /**
     * Construct a {@link CommandError} from a {@link Throwable}.
     *
     * @param reason the reason value
     * @param throwable for more context
     * @return the constructed {@link CommandError}
     */
    public static CommandError of(final Reason reason, final Throwable throwable) {
        return new CommandError(Error.of(reason, throwable));
    }

    /**
     * Construct a {@link CommandError} from a {@link Throwable}.
     *
     * @param reason the reason value
     * @param msg for more context
     * @return the constructed {@link CommandError}
     */
    public static CommandError of(final Reason reason, final String msg) {
        return new CommandError(Error.of(reason, msg));
    }

    /**
     * The reason value accessor.
     *
     * @return the reason
     */
    public Reason getReason() {
        return error.getReason();
    }

    /**
     * The error message accessor
     *
     * @return the error message
     */
    public String getMessage() {
        return error.getMessage();
    }

    private final Error<Reason> error;

    private CommandError(final Error<Reason> error) {
        this.error = error;
    }

    public enum Reason {
        InvalidCommand,
        InvalidReadSequence,
        CommandHandlerFailed,
        AggregateNotFound,
        Timeout,
        RemoteLookupFailed,
        CommandPublishError,
        InternalError,
        UnexpectedErrorCode
    }
}
