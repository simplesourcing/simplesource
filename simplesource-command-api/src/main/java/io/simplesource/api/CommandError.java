package io.simplesource.api;

import java.util.Objects;
import java.util.function.BiFunction;

import static java.util.Objects.requireNonNull;

/**
 * A CommandError explains failures. They can be constructed from the reason type,
 * with either a {@link String} or a {@link Throwable} for more context.
 */
public abstract class CommandError {

    /**
     * Construct a {@link CommandError} from a {@link Throwable}
     *
     * @param reason the reason value
     * @param throwable for more context
     * @return the constructed {@link CommandError}
     */
    public static CommandError of(final Reason reason, final Throwable throwable) {
        return new ThrowableCommandError(reason, throwable);
    }

    /**
     * Construct a {@link CommandError} from a {@link Throwable}
     *
     * @param reason the reason value
     * @param msg for more context
     * @return the constructed {@link CommandError}
     */
    public static CommandError of(final Reason reason, final String msg) {
        return new StringCommandError(reason, msg);
    }

    /**
     * The reason value acaessor
     *
     * @return the reason
     */
    public Reason getReason() {
        return reason;
    };

    /**
     * The reason message acaessor
     *
     * @return the reason message
     */
    public abstract String getMessage();

    /**
     * Access the reason value and either the string or the throwable context and return whatever you like.
     *
     * @param <A> the result type
     * @param str the function that receives the reason and string, returning a value of the specified type
     * @param ex the function that receives the reason and throwable, returning a value of the specified type
     * @return the result
     */
    public abstract <A> A fold(BiFunction<Reason, String, A> str, BiFunction<Reason, Throwable, A> ex);

    //
    // internals
    //

    private final Reason reason;

    private CommandError(final Reason reason) {
        this.reason = reason;
    }

    @Override
    public String toString() {
        return "CommandError(" + fold(
                (error, message) -> "Error: " + error + " Message: " + message,
                (error, throwable) -> "Error: " + error + " Throwable: " + throwable) + ')';
    }

    static final class ThrowableCommandError extends CommandError {
        private final Throwable throwable;

        ThrowableCommandError(final Reason reason, final Throwable throwable) {
            super(reason);
            this.throwable = requireNonNull(throwable);
        }

        @Override
        public String getMessage() {
            return throwable.getMessage();
        }

        @Override
        public <A> A fold(final BiFunction<Reason, String, A> str, final BiFunction<Reason, Throwable, A> ex) {
            return ex.apply(getReason(), throwable);
        }

        @Override
        public int hashCode() {
            return 31 * getReason().hashCode() + throwable.hashCode();
        }

        @Override
        public boolean equals(final Object obj) {
            if (!(obj instanceof CommandError.ThrowableCommandError))
                return false;
            final ThrowableCommandError other = (ThrowableCommandError) obj;
            return Objects.equals(getReason(), other.getReason()) &&
                    Objects.equals(throwable, other.throwable);
        }

    }

    static final class StringCommandError extends CommandError {
        private final String msg;

        StringCommandError(final Reason reason, final String msg) {
            super(reason);
            this.msg = requireNonNull(msg);
        }

        @Override
        public String getMessage() {
            return msg;
        }

        @Override
        public <A> A fold(final BiFunction<Reason, String, A> str, final BiFunction<Reason, Throwable, A> ex) {
            return str.apply(getReason(), msg);
        }

        @Override
        public int hashCode() {
            return 31 * getReason().hashCode() + msg.hashCode();
        }

        @Override
        public boolean equals(final Object obj) {
            if (!(obj instanceof CommandError.StringCommandError))
                return false;
            final StringCommandError other = (StringCommandError) obj;
            return Objects.equals(getReason(), other.getReason()) &&
                    Objects.equals(msg, other.msg);
        }

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
