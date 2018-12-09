package io.simplesource.data;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * A CommandError explains failures. They can be constructed from the reason type,
 * with either a {@link String} or a {@link Throwable} for more context.
 */
public interface Error<Reason> {

    /**
     * Construct a {@link Error} from a {@link Throwable}.
     *
     * @param reason the reason value
     * @param throwable for more context
     * @return the constructed {@link Error}
     */
    static <Reason> Error<Reason> of(final Reason reason, final Throwable throwable) {
        return new ThrowableError<>(reason, throwable);
    }

    /**
     * Construct a {@link Error} from a {@link Throwable}.
     *
     * @param reason the reason value
     * @param msg for more context
     * @return the constructed {@link Error}
     */
    static <Reason> Error<Reason> of(final Reason reason, final String msg) {
        return new StringError<>(reason, msg);
    }

    /**
     * The reason value accessor.
     *
     * @return the reason
     */
    Reason getReason();

    /**
     * The reason message accessor
     *
     * @return the reason message
     */
    String getMessage();

    //
    // implementations
    //
    final class ThrowableError<Reason>  implements Error<Reason>  {
        private final Throwable throwable;
        private final Reason reason;

        ThrowableError(final Reason reason, final Throwable throwable) {
            this.reason = reason;
            this.throwable = requireNonNull(throwable);
        }

        @Override
        public String getMessage() {
            return throwable.getMessage();
        }

        @Override
        public Reason getReason() {
            return reason;
        }

        @Override
        public int hashCode() {
            return 31 * getReason().hashCode() + throwable.hashCode();
        }

        @Override
        public boolean equals(final Object obj) {
            if (!(obj instanceof ThrowableError))
                return false;
            final ThrowableError other = (ThrowableError) obj;
            return Objects.equals(getReason(), other.getReason()) &&
                    Objects.equals(throwable, other.throwable);
        }

        @Override
        public String toString() {
            return "Error(" + "Reason: " + getReason() + " Throwable: " + throwable + ')';
        }
    }

    final class StringError<Reason>  implements Error<Reason>  {
        private final String msg;
        private final Reason reason;

        StringError(final Reason reason, final String msg) {
            this.reason = reason;
            this.msg = requireNonNull(msg);
        }

        @Override
        public String getMessage() {
            return msg;
        }

        @Override
        public Reason getReason() {
            return reason;
        }

        @Override
        public int hashCode() {
            return 31 * getReason().hashCode() + msg.hashCode();
        }

        @Override
        public boolean equals(final Object obj) {
            if (!(obj instanceof StringError))
                return false;
            final StringError<?> other = (StringError<?>) obj;
            return Objects.equals(getReason(), other.getReason()) &&
                    Objects.equals(msg, other.msg);
        }

        @Override
        public String toString() {
            return "Error(" + "Reason: " + getReason() + " Message: " + getMessage() + ')';
        }
    }
}
