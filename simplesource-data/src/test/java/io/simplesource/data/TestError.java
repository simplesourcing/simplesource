package io.simplesource.data;

public class TestError {
    private final Reason reason;

    public TestError(Reason reason) {
        this.reason = reason;
    }

    public Reason getReason() {
        return reason;
    }

    enum Reason {
        InternalError,
        UnexpectedErrorCode,
        Timeout
    }
}
