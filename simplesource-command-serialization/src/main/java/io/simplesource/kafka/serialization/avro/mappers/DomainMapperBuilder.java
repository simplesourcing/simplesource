package io.simplesource.kafka.serialization.avro.mappers;

import io.simplesource.kafka.serialization.avro.AvroSpecificGenericMapper;
import io.simplesource.kafka.serialization.avro.mappers.steps.DomainSerializedClassMapperToSerializedStep;
import io.simplesource.kafka.serialization.util.GenericMapper;
import org.apache.avro.generic.GenericRecord;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;


public class DomainMapperBuilder {
    private final DomainMapperRegistry domainMapperRegistry;
    private BuildConfig buildConfig;

    public DomainMapperBuilder() {
        this(new DomainMapperRegistry());
    }

    public DomainMapperBuilder(DomainMapperRegistry domainMapperRegistry) {
        this.domainMapperRegistry = domainMapperRegistry;
        this.buildConfig = new BuildConfig();
    }

    public <D, A extends GenericRecord> DomainSerializedClassMapperToSerializedStep<D, A> mapperFor(Class<D> domainClass, Class<A> serializedClass) {
        return new DomainSerializedClassMapperImpl<D, A>(this, domainMapperRegistry)
                .mapperFor(requireNonNull(domainClass), requireNonNull(serializedClass));
    }

    public <D, A extends GenericRecord> DomainSerializedClassMapperToSerializedStep<Optional<D>, A> optionalMapperFor(Class<D> domainClass, Class<A> serializedClass) {
        //For cases where Optional is empty, we return the base class of all mapped types. This is useful for cases when
        //map only one type like aggregate or aggregate key
        buildConfig.setDomainValueToClassFunc((Optional<D> v) -> v.<Class<?>>map(D::getClass).orElse(domainClass));
        return new DomainSerializedClassMapperImpl<Optional<D>, A>(this, domainMapperRegistry)
                .mapperFor(requireNonNull(domainClass), requireNonNull(serializedClass));
    }

    public DomainMapperBuilder withExceptionSupplierForNotRegisteredMapper(Supplier<RuntimeException> exceptionSupplier) {
        buildConfig.exceptionSupplier = Optional.of(requireNonNull(exceptionSupplier));
        return this;
    }

    public <D, A extends GenericRecord> GenericMapper<D, A> build() {
        return build(AvroSpecificGenericMapper.specificDomainMapper());
    }

    public <D, A extends GenericRecord> GenericMapper<D, A> build(GenericMapper<A, GenericRecord> avroSpecificGenericMapper) {
        RegistryBasedDomainMapper<D, A> domainMapper = new RegistryBasedDomainMapper<>(domainMapperRegistry, avroSpecificGenericMapper, buildConfig.getDomainValueToClassFunc());
        buildConfig.exceptionSupplier.ifPresent(domainMapper::withExceptionSupplier);
        return domainMapper;
    }

    private class BuildConfig {
        Optional<Supplier<RuntimeException>> exceptionSupplier = Optional.empty();
        //Workaround to store the function and able to cast to required type
        private Function<?, Class<?>> domainValueToClassFunc = Object::getClass;

        <D> Function<D, Class<?>> getDomainValueToClassFunc() {
            return (Function<D, Class<?>>) domainValueToClassFunc;
        }

        <D> void setDomainValueToClassFunc(Function<D, Class<?>> domainValueToClassFunc) {
            this.domainValueToClassFunc = domainValueToClassFunc;
        }
    }
}
