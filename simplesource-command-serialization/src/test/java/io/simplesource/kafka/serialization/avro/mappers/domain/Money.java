package io.simplesource.kafka.serialization.avro.mappers.domain;

import com.google.common.base.Strings;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class Money implements Comparable<Money>{
    public static final int DEFAULT_SCALE = 4;
    public static final Money ZERO = Money.valueOf("0");

    private BigDecimal amount;

    private Money(BigDecimal amount) {
        this.amount = amount.setScale(DEFAULT_SCALE, RoundingMode.CEILING);
    }

    public static Money valueOf(BigDecimal amount) {
        requireNonNull(amount);
        return new Money(amount);
    }

    public static Money valueOf(String amount) {
        checkArgument(!Strings.isNullOrEmpty(amount));

        return new Money(new BigDecimal(amount));
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public Money add(Money diff) {
        return new Money(this.amount.add(diff.getAmount()));
    }

    public Money subtract(Money diff) {
        return new Money(this.amount.subtract(diff.getAmount()));
    }

    public boolean isNegativeAmount() {
        return this.compareTo(ZERO) < 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Money money = (Money) o;
        return Objects.equals(amount, money.amount);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(amount);
    }

    @Override
    public String toString() {
        return amount.toString();
    }

    @Override
    public int compareTo(Money o) {
        return this.amount.compareTo(o.amount);
    }
}
