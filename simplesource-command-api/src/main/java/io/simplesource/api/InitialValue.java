package io.simplesource.api;

/**
 * Provides the initial value of an aggregate .
 *
 * @param <K> The key type for th aggregate.
 * @param <A> The aggregate type.
 */
@FunctionalInterface
public interface InitialValue<K, A> {
    A empty(K key);
}
