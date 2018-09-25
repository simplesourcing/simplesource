package io.simplesource.kafka.serialization.avro.mappers;

import lombok.Value;
import org.apache.avro.generic.GenericRecord;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.common.collect.Maps.newHashMap;
import static java.util.Objects.requireNonNull;

public final class DomainMapperRegistry {
    private final Map<RegisterKey, RegistryMapper> mappers = newHashMap();

    public <D, A extends GenericRecord> Optional<RegistryMapper<D, A>> mapperFor(Class clazz) {
        Class classToSearchFor = requireNonNull(clazz);
        return mappers.entrySet().stream().filter(byClass(classToSearchFor))
                .<RegistryMapper<D, A>>map(Map.Entry::getValue).findAny();
    }

    public <D, A extends GenericRecord> DomainMapperRegistry register(Class<?> domainClazz, Class<?> avroClazz,
                                                                      Function<D, A> fromDomain,
                                                                      Function<A, D> toDomain) {
        mappers.put(new RegisterKey(requireNonNull(domainClazz), requireNonNull(avroClazz)),
                new RegistryMapper<>(requireNonNull(fromDomain), requireNonNull(toDomain)));
        return this;
    }

    @Value
    private class RegisterKey {
        final Class<?> domainClass;
        final Class<?> serializedClass;
    }

    @Value
    static final class RegistryMapper<D, A> {
        final Function<D, A> fromDomainFunc;
        final Function<A, D> toDomainFunc;
    }

    private Predicate<Map.Entry<RegisterKey,RegistryMapper>> byClass(Class clazz) {
        return e -> e.getKey().domainClass().equals(clazz) || e.getKey().serializedClass().equals(clazz);
    }
}
