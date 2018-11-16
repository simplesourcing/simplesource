package io.simplesource.kafka.internal.client;

import io.simplesource.data.FutureResult;
import lombok.Value;

public interface RequestSender<K, V> {
    @Value
    public static class SendResult {
        long timeStamp;
    }

    FutureResult<Exception, SendResult> send(K key, V value);
}
