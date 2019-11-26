package io.simplesource.kafka.internal.client;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import lombok.val;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import io.simplesource.kafka.internal.client.KafkaRequestAPI.ResponseHandler;

final class CachedResponseHandlers<RK, I, R> {

    private final Cache<RK, ResponseHandler<I, R>> innerCache;

    CachedResponseHandlers(long retentionInSeconds) {
        innerCache = CacheBuilder
                .newBuilder()
                .expireAfterWrite(retentionInSeconds, TimeUnit.SECONDS)
                .build();
    }

    final void insertIfAbsent(RK k, Supplier<ResponseHandler<I, R>> lazyV) {
        val oldV     = innerCache.getIfPresent(k);
        val isAbsent = oldV == null;
        if (isAbsent) {
            val v = lazyV.get();
            if (v != null) {
                innerCache.put(k, v);
            }
        }
    }

    final ResponseHandler<I, R> computeIfPresent(RK k, Function<ResponseHandler<I, R>, ResponseHandler<I, R>> vToV) {
        val oldV      = innerCache.getIfPresent(k);
        val isPresent = oldV != null;
        if (isPresent) {
            val newV = vToV.apply(oldV);
            if (newV != null) {
                innerCache.put(k, newV);
                return newV;
            }
        }
        return null;
    }

    final void removeAll(Consumer<ResponseHandler<I, R>> consumeV) {
        innerCache.asMap().values().forEach(consumeV);
        innerCache.invalidateAll();
    }

}
