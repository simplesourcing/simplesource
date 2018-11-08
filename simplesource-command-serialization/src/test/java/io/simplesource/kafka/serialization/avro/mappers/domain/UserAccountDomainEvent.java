package io.simplesource.kafka.serialization.avro.mappers.domain;

import lombok.Value;

import java.math.BigDecimal;

public interface UserAccountDomainEvent {
    @Value
    class AccountCreated implements UserAccountDomainEvent {
        final String name;
        final Money balance;
    }
    @Value
    class UserNameUpdated implements UserAccountDomainEvent {
        final String name;
    }
    @Value
    class AccountDeleted implements UserAccountDomainEvent {
    }
    @Value
    class BuggyEvent implements UserAccountDomainEvent {
    }
}
