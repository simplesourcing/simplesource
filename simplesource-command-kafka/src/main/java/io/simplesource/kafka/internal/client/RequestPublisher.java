package io.simplesource.kafka.internal.client;

import io.simplesource.data.FutureResult;
import lombok.Value;

public interface RequestPublisher<K, V> {
    @Value
    public static class SendResult {
        long timeStamp;
    }

    FutureResult<Exception, SendResult> publish(K key, V value);
}
