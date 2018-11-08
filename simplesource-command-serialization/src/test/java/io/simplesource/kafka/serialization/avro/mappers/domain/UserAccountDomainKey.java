package io.simplesource.kafka.serialization.avro.mappers.domain;

import lombok.Value;

@Value
public final class UserAccountDomainKey {
    final String userId;
}
