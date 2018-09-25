package io.simplesource.data;

import java.util.Objects;
import java.util.function.BiFunction;

import static java.util.Objects.requireNonNull;

/**
 * A Reason explains failures. They can be constructed from the error type,
 * with either a {@link String} or a {@link Throwable} for more context.
 */
public abstract class Reason<E> {

    /**
     * Construct a {@link Reason} from a {@link Throwable}
     *
     * @param error the error value
     * @param throwable for more context
     * @param <E> the error type
     * @return the constructed {@link Reason}
     */
    public static <E> Reason<E> of(final E error, final Throwable throwable) {
        return new ThrowableReason<>(error, throwable);
    }

    /**
     * Construct a {@link Reason} from a {@link Throwable}
     *
     * @param error the error value
     * @param msg for more context
     * @param <E> the error type
     * @return the constructed {@link Reason}
     */
    public static <E> Reason<E> of(final E error, final String msg) {
        return new StringReason<>(error, msg);
    }

    /**
     * The error value acaessor
     *
     * @return the error
     */
    public E getError() {
        return error;
    };

    /**
     * The error message acaessor
     *
     * @return the error message
     */
    public abstract String getMessage();

    /**
     * Access the error value and either the string or the throwable context and return whatever you like.
     *
     * @param str the function that receives the error and string, returning a value of the specified type
     * @param ex the function that receives the error and throwable, returning a value of the specified type
     * @param <A> the result type
     * @return the result
     */
    public abstract <A> A fold(BiFunction<E, String, A> str, BiFunction<E, Throwable, A> ex);

    //
    // internals
    //

    private final E error;

    private Reason(final E error) {
        this.error = error;
    }

    @Override
    public String toString() {
        return "Reason(" + fold(
                (error, message) -> "Error: " + error + " Message: " + message,
                (error, throwable) -> "Error: " + error + " Throwable: " + throwable) + ')';
    }

    static final class ThrowableReason<E> extends Reason<E> {
        private final Throwable throwable;

        ThrowableReason(final E error, final Throwable throwable) {
            super(error);
            this.throwable = requireNonNull(throwable);
        }

        @Override
        public String getMessage() {
            return throwable.getMessage();
        }

        @Override
        public <A> A fold(final BiFunction<E, String, A> str, final BiFunction<E, Throwable, A> ex) {
            return ex.apply(getError(), throwable);
        }

        @Override
        public int hashCode() {
            return 31 * getError().hashCode() + throwable.hashCode();
        }

        @Override
        public boolean equals(final Object obj) {
            if (!(obj instanceof ThrowableReason))
                return false;
            final ThrowableReason<E> other = (ThrowableReason<E>) obj;
            return Objects.equals(getError(), other.getError()) &&
                    Objects.equals(throwable, other.throwable);
        }

    }

    static final class StringReason<E> extends Reason<E> {
        private final String msg;

        StringReason(final E error, final String msg) {
            super(error);
            this.msg = requireNonNull(msg);
        }

        @Override
        public String getMessage() {
            return msg;
        }

        @Override
        public <A> A fold(final BiFunction<E, String, A> str, final BiFunction<E, Throwable, A> ex) {
            return str.apply(getError(), msg);
        }

        @Override
        public int hashCode() {
            return 31 * getError().hashCode() + msg.hashCode();
        }

        @Override
        public boolean equals(final Object obj) {
            if (!(obj instanceof StringReason))
                return false;
            final StringReason<E> other = (StringReason<E>) obj;
            return Objects.equals(getError(), other.getError()) &&
                    Objects.equals(msg, other.msg);
        }

    }
}
