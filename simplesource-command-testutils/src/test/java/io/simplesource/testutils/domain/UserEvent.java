package io.simplesource.testutils.domain;


import io.simplesource.api.Aggregator;
import io.simplesource.dsl.AggregatorBuilder;
import lombok.Value;

import java.util.Optional;

import static java.util.Optional.empty;

public interface UserEvent {

    @Value
    class UserInserted implements UserEvent {
        private final String firstName;
        private final String lastName;
    }

    @Value
    class FirstNameUpdated implements UserEvent {
        private final String firstName;
    }

    @Value
    class LastNameUpdated implements UserEvent {
        private final String lastName;
    }

    @Value
    class YearOfBirthUpdated implements UserEvent {
        private final Integer yearOfBirth;
    }

    @Value
    class UserDeleted implements UserEvent {
    }

    @Value
    class BuggyEvent implements UserEvent {
    }


    static Aggregator<BuggyEvent, Optional<User>> handleBuggyEvent() {
        return (currentAggregate, event) -> {
            throw new UnsupportedOperationException();
        };
    }

    static Aggregator<UserDeleted, Optional<User>> handleUserDeleted() {
        return (currentAggregate, event) -> empty();
    }

    static Aggregator<YearOfBirthUpdated, Optional<User>> handleYearOfBirthUpdated() {
        return (currentAggregate, event) ->
                currentAggregate.map(user -> user.toBuilder()
                        .yearOfBirth(event.yearOfBirth())
                        .build());
    }

    static Aggregator<LastNameUpdated, Optional<User>> handleLastNameUpdated() {
        return (currentAggregate, event) ->
                currentAggregate.map(user -> user.toBuilder()
                        .lastName(event.lastName())
                        .build());
    }

    static Aggregator<FirstNameUpdated, Optional<User>> handleFirstNameUpdated() {
        return (currentAggregate, event) ->
                currentAggregate.map(user -> user.toBuilder()
                        .firstName(event.firstName())
                        .build());
    }

    static Aggregator<UserInserted, Optional<User>> handleUserInserted() {
        return (currentAggregate, event) ->
                Optional.of(new User(event.firstName(), event.lastName(), null));
    }


    static Aggregator<UserEvent, Optional<User>> getAggregator() {
        return AggregatorBuilder.<UserEvent, Optional<User>> newBuilder()
                .onEvent(UserInserted.class, handleUserInserted())
                .onEvent(FirstNameUpdated.class, handleFirstNameUpdated())
                .onEvent(LastNameUpdated.class, handleLastNameUpdated())
                .onEvent(YearOfBirthUpdated.class, handleYearOfBirthUpdated())
                .onEvent(UserDeleted.class, handleUserDeleted())
                .onEvent(BuggyEvent.class, handleBuggyEvent())
                .build();
    }

}
