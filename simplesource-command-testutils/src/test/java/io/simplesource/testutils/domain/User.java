package io.simplesource.testutils.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
@AllArgsConstructor
public final class User {
    private final String firstName;
    private final String lastName;
    private final Integer yearOfBirth;
}
