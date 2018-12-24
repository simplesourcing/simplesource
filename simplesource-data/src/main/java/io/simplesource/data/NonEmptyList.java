package io.simplesource.data;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * A random access <code>List</code> implementation that is guaranteed to contain at least one item.
 *
 * @param <A> the type of elements in this list
 */
public final class NonEmptyList<A> extends AbstractList<A> {

    private final A head;
    private final List<A> tail;

    @SafeVarargs
    public static <A> NonEmptyList<A> of(final A a, final A... as) {
        return new NonEmptyList<>(a, Arrays.asList(as));
    }

    public static <A> Optional<NonEmptyList<A>> fromList(List<A> l) {

        if (l.size() < 1) return Optional.empty();

        if (l.size() == 1) {
            return Optional.of(NonEmptyList.of(l.get(0)));
        }

        return Optional.of(new NonEmptyList<>(l.get(0), l.subList(1, l.size())));
    }

    public NonEmptyList(final A head, final List<A> tail) {
        this.head = requireNonNull(head);
        this.tail = requireNonNull(tail);
    }

    @Override
    public A get(final int index) {
        return index == 0 ? head : tail.get(index - 1);
    }

    @Override
    public int size() {
        return tail.size() + 1;
    }

    @Override
    public boolean removeIf(final Predicate<? super A> filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void replaceAll(final UnaryOperator<A> operator) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sort(final Comparator<? super A> c) {
        throw new UnsupportedOperationException();
    }

    public <B> NonEmptyList<B> map(final Function<A, B> op) {
        final B head = op.apply(this.head);
        final List<B> tail = this.tail.stream().map(op).collect(Collectors.toList());
        return new NonEmptyList<>(head, tail);
    }

    public A head() {
        return head;
    }

    public A last() {
        return tail.isEmpty() ? head : tail.get(tail.size() - 1);
    }

    public List<A> tail() {
        return tail;
    }

    public List<A> toList() {
        List<A> newList = new ArrayList<>();
        newList.add(head);
        newList.addAll(tail);
        return newList;
    }

    public <B> B fold(final Function<A, B> initialResult, final BiFunction<B, A, B> op) {
        B result = initialResult.apply(head);
        for (final A a : tail) {
            result = op.apply(result, a);
        }
        return result;
    }

    public <B> B foldLeft(final B b, final BiFunction<B, A, B> op) {
        B result = b;
        for (final A a : this) {
            result = op.apply(result, a);
        }
        return result;
    }

}
