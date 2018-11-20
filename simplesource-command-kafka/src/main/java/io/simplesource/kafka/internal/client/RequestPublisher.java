package io.simplesource.kafka.internal.client;

import io.simplesource.data.FutureResult;
import lombok.Value;

public interface RequestPublisher<K, V> {
    @Value
    class PublishResult {
        long timeStamp;
    }

    FutureResult<Exception, PublishResult> publish(K key, V value);
}
