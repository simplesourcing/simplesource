package io.simplesource.api;

import io.simplesource.data.Error;
import lombok.Value;

import java.util.function.BiFunction;

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
        return reason.getReason();
    }

    /**
     * The reason message accessor
     *
     * @return the reason message
     */
    public String getMessage() {
        return reason.getMessage();
    }

    /**
     * Access the reason value and either the string or the throwable context and return whatever you like.
     *
     * @param <A> the result type
     * @param str the function that receives the reason and string, returning a value of the specified type
     * @param ex the function that receives the reason and throwable, returning a value of the specified type
     * @return the result
     */
    public <A> A fold(BiFunction<Reason, String, A> str, BiFunction<Reason, Throwable, A> ex){
        return reason.fold(str, ex);
    }

    private final Error<Reason> reason;

    private CommandError(final Error<Reason> reason) {
        this.reason = reason;
    }

    @Override
    public String toString() {
        return "CommandError(" + fold(
                (error, message) -> "Error: " + error + " Message: " + message,
                (error, throwable) -> "Error: " + error + " Throwable: " + throwable) + ')';
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
