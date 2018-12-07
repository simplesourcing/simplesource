package io.simplesource.api;

/**
 * Provides the initial value of an aggregate.
 *
 * @param <K> the aggregate key type
 * @param <A> the aggregate type
 */
@FunctionalInterface
public interface InitialValue<K, A> {
    A empty(K key);
}
