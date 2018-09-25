package io.simplesource.kafka.serialization.avro.mappers;

import io.simplesource.kafka.serialization.util.GenericMapper;
import org.apache.avro.generic.GenericRecord;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Domain mapper implementation using registry for all supported domain-serialized class combinations
 * @param <V> Base class for all supported domain classes in the registry
 * @param <S> Avro base serialized class. Most of the cases this will be {@link GenericRecord}
 */
public final class RegistryBasedDomainMapper<V, S extends GenericRecord> implements GenericMapper<V, S> {
    private final DomainMapperRegistry domainMapperRegistry;
    private final GenericMapper<S, GenericRecord> avroSpecificGenericMapper;
    private final Function<V, Class<?>> domainValueToTypeFunct;
    private Supplier<? extends RuntimeException> exceptionSupplier;

    RegistryBasedDomainMapper(DomainMapperRegistry domainMapperRegistry, GenericMapper<S, GenericRecord> avroSpecificGenericMapper,
                              Function<V, Class<?>> f) {
        this.domainMapperRegistry = domainMapperRegistry;
        this.avroSpecificGenericMapper = avroSpecificGenericMapper;
        domainValueToTypeFunct = f;
        this.exceptionSupplier = () -> new IllegalArgumentException("Class not supported");
    }

    @Override
    public S toGeneric(V value) {
        return lookupMapperInRegistry(domainValueToTypeFunct.apply(value)).fromDomainFunc().apply(value);
    }

    @Override
    public V fromGeneric(S serialized) {
        final S specificRecord = avroSpecificGenericMapper.fromGeneric(serialized);
        return lookupMapperInRegistry(specificRecord.getClass()).toDomainFunc().apply(specificRecord);
    }

    void withExceptionSupplier(Supplier<? extends RuntimeException> exceptionSupplier) {
        this.exceptionSupplier = exceptionSupplier;
    }

    private DomainMapperRegistry.RegistryMapper<V, S> lookupMapperInRegistry(Class<?> typeClass) {
        Optional<DomainMapperRegistry.RegistryMapper<V, S>> registryMapper = domainMapperRegistry.mapperFor(typeClass);
        return registryMapper.orElseThrow(exceptionSupplier);
    }
}
