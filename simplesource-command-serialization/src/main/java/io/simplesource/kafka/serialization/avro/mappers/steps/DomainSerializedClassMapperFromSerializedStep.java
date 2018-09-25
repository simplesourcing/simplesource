package io.simplesource.kafka.serialization.avro.mappers.steps;

import org.apache.avro.generic.GenericRecord;

import java.util.function.Function;

public interface DomainSerializedClassMapperFromSerializedStep<D, A extends GenericRecord> {
    DomainSerializedClassMapperRegisterStep fromSerialized(Function<A, D> toDomain);
}
