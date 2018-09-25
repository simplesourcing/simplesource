package io.simplesource.kafka.serialization.avro.mappers.steps;

import io.simplesource.kafka.serialization.avro.mappers.DomainMapperBuilder;

public interface DomainSerializedClassMapperRegisterStep {
    DomainMapperBuilder register();
}
