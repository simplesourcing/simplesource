package io.simplesource.api;

import lombok.EqualsAndHashCode;

/**
 * A CommandError explains failures.
 */
@EqualsAndHashCode
public abstract class CommandError extends RuntimeException {

    protected CommandError() {
        super();
    }

    protected CommandError(String message) {
        super(message);
    }

    protected CommandError(Exception exception) {
        super(exception);
    }

    public static final class InvalidCommand extends CommandError {
        private static final long serialVersionUID = -1790183317621500950L;

        public InvalidCommand() {
            super();
        }

        public InvalidCommand(String message) {
            super(message);
        }

        public InvalidCommand(Exception exception) {
            super(exception);
        }
    }

    public static final class InvalidReadSequence extends CommandError {
        private static final long serialVersionUID = -2496767690062272814L;

        public InvalidReadSequence() {
            super();
        }

        public InvalidReadSequence(String message) {
            super(message);
        }

        public InvalidReadSequence(Exception exception) {
            super(exception);
        }
    }

    public static final class CommandHandlerFailed extends CommandError {
        private static final long serialVersionUID = 5243263599400133549L;

        public CommandHandlerFailed() {
            super();
        }

        public CommandHandlerFailed(String message) {
            super(message);
        }

        public CommandHandlerFailed(Exception exception) {
            super(exception);
        }
    }

    public static final class AggregateNotFound extends CommandError {
        private static final long serialVersionUID = -8322425056292085507L;

        public AggregateNotFound() {
            super();
        }

        public AggregateNotFound(String message) {
            super(message);
        }

        public AggregateNotFound(Exception exception) {
            super(exception);
        }
    }

    public static final class Timeout extends CommandError {
        private static final long serialVersionUID = 2571850763360665431L;

        public Timeout() {
            super();
        }

        public Timeout(String message) {
            super(message);
        }

        public Timeout(Exception exception) {
            super(exception);
        }
    }

    public static final class RemoteLookupFailed extends CommandError {
        private static final long serialVersionUID = 5877483197829555210L;

        public RemoteLookupFailed() {
            super();
        }

        public RemoteLookupFailed(String message) {
            super(message);
        }

        public RemoteLookupFailed(Exception exception) {
            super(exception);
        }
    }

    public static final class CommandPublishError extends CommandError {
        private static final long serialVersionUID = -6900063712198941295L;

        public CommandPublishError() {
            super();
        }

        public CommandPublishError(String message) {
            super(message);
        }

        public CommandPublishError(Exception exception) {
            super(exception);
        }
    }

    public static final class InternalError extends CommandError {
        private static final long serialVersionUID = -6091546263840539333L;

        public InternalError() {
            super();
        }

        public InternalError(String message) {
            super(message);
        }

        public InternalError(Exception exception) {
            super(exception);
        }
    }

    public static final class UnhandledCommandType extends CommandError {
        private static final long serialVersionUID = -1977890441958111949L;

        public UnhandledCommandType() {
            super();
        }

        public UnhandledCommandType(String message) {
            super(message);
        }

        public UnhandledCommandType(Exception exception) {
            super(exception);
        }
    }

}

