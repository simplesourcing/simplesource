package io.simplesource.kafka.serialization.avro.mappers;

import io.simplesource.kafka.serialization.avro.mappers.steps.DomainSerializedClassMapperFromSerializedStep;
import io.simplesource.kafka.serialization.avro.mappers.steps.DomainSerializedClassMapperRegisterStep;
import io.simplesource.kafka.serialization.avro.mappers.steps.DomainSerializedClassMapperToSerializedStep;
import org.apache.avro.generic.GenericRecord;

import java.util.function.Function;

import static java.util.Objects.requireNonNull;

class DomainSerializedClassMapperImpl<D, A extends GenericRecord> implements DomainSerializedClassMapper<D, A>, DomainSerializedClassMapperToSerializedStep<D, A>,
        DomainSerializedClassMapperFromSerializedStep<D, A>, DomainSerializedClassMapperRegisterStep {
    private Class<?> domainClass;
    private Class<?> serializedClass;
    private Function<D, A> fromDomain;
    private Function<A, D> toDomain;
    private DomainMapperBuilder domainMapperBuilder;
    private DomainMapperRegistry domainMapperRegistry;

    DomainSerializedClassMapperImpl(DomainMapperBuilder domainMapperBuilder, DomainMapperRegistry domainMapperRegistry) {
        this.domainMapperBuilder = domainMapperBuilder;
        this.domainMapperRegistry = domainMapperRegistry;
    }

    @Override
    public DomainSerializedClassMapperToSerializedStep<D, A> mapperFor(Class<?> domainClass, Class<?> serializedClass) {
        this.domainClass = requireNonNull(domainClass);
        this.serializedClass = requireNonNull(serializedClass);
        return this;
    }

    @Override
    public DomainSerializedClassMapperFromSerializedStep<D, A> toSerialized(Function<D, A> mapFunc) {
        this.fromDomain = requireNonNull(mapFunc);
        return this;
    }

    @Override
    public DomainSerializedClassMapperRegisterStep fromSerialized(Function<A, D> toDomain) {
        this.toDomain = requireNonNull(toDomain);
        return this;
    }

    @Override
    public DomainMapperBuilder register() {
        this.domainMapperRegistry.register(domainClass, serializedClass, fromDomain, toDomain);
        this.clear();
        return domainMapperBuilder;
    }

    private void clear() {
        this.domainClass = null;
        this.toDomain = null;
        this.fromDomain = null;
        this.serializedClass = null;
    }
}
