package io.simplesource.kafka.internal.util;

@FunctionalInterface
public interface RetryDelay {

    Long delay(long startTime, long timeoutMillis, int spinCount);

}
