package io.simplesource.kafka.serialization.avro.mappers.domain;

import com.fasterxml.jackson.databind.jsonschema.JsonSerializableSchema;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Value;

import java.math.BigDecimal;

public interface UserAccountDomainCommand {
    @Value
    class CreateAccount implements UserAccountDomainCommand {
        final String name;
        final Money balance;
    }

    @Value
    class UpdateUserName implements UserAccountDomainCommand {
        final String name;
    }

    @Value
    class DeleteAccount implements UserAccountDomainCommand {
    }

    @Value
    class BuggyCommand implements UserAccountDomainCommand {
    }
}
