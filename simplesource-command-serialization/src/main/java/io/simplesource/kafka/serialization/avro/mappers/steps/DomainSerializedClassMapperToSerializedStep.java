package io.simplesource.kafka.serialization.avro.mappers.steps;

import org.apache.avro.generic.GenericRecord;

import java.util.function.Function;

public interface DomainSerializedClassMapperToSerializedStep<D, A extends GenericRecord> {
    DomainSerializedClassMapperFromSerializedStep<D, A> toSerialized(Function<D, A> mapFunc);
}
