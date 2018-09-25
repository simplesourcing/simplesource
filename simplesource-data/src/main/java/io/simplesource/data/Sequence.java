package io.simplesource.data;

import java.util.Objects;

/**
 * A monotonically increasing identifier. Each event for a given aggregate has a Sequence starting with 0 for the first
 * event and increasing by one for each event thereafter.
 */
public final class Sequence implements Comparable<Sequence> {

    public static Sequence first() {
        return new Sequence(0L);
    }

    public static Sequence position(final long pos) {
        return new Sequence(pos);
    }

    private final long seq;

    private Sequence(final long seq) {
        this.seq = seq;
    }

    public long getSeq() {
        return seq;
    }

    public Sequence next() {
        return new Sequence(seq + 1);
    }

    @Override
    public int compareTo(final Sequence o) {
        return Long.compare(seq, o.seq);
    }

    public boolean isEqualTo(final Sequence sequence) {
        return this.seq == sequence.seq;
    }

    public boolean isGreaterThan(final Sequence sequence) {
        return this.seq > sequence.seq;
    }

    public boolean isGreaterThanOrEqual(final Sequence sequence) {
        return this.seq >= sequence.seq;
    }

    public boolean isLessThan(final Sequence sequence) {
        return this.seq < sequence.seq;
    }

    public boolean isLessThanOrEqual(final Sequence sequence) {
        return this.seq <= sequence.seq;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || !Objects.equals(getClass(), o.getClass())) return false;

        final Sequence sequence = (Sequence) o;

        return seq == sequence.seq;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(seq);
    }

    @Override
    public String toString() {
        return "io.simplesource.data.Sequence{" +
                "seq=" + seq +
                '}';
    }
}
