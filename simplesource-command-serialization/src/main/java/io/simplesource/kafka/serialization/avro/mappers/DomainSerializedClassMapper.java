package io.simplesource.kafka.serialization.avro.mappers;

import io.simplesource.kafka.serialization.avro.mappers.steps.DomainSerializedClassMapperToSerializedStep;
import org.apache.avro.generic.GenericRecord;

/**
 * Mapper for domain and serialized class pairs. This is part of DSL for mapping domain classes to serialized ones and
 * vice versa.
 * @param <D> Domain class
 * @param <A> Avro serialized class
 */
public interface DomainSerializedClassMapper<D, A extends GenericRecord> {
    DomainSerializedClassMapperToSerializedStep<D, A> mapperFor(Class<?> domainClass, Class<?> serializedClass);
}
