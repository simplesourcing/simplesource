package io.simplesource.kafka.serialization.avro.mappers;

import lombok.Value;
import org.apache.avro.generic.GenericRecord;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.common.collect.Maps.newHashMap;
import static java.util.Objects.requireNonNull;

public class DomainMapperRegistry {
    private final Map<RegisterKey, RegisterMapper> mappers = newHashMap();

    public <D, A extends GenericRecord> Optional<RegisterMapper<D, A>> mapperFor(Class clazz) {
        return mappers.entrySet().stream().filter(byClass(requireNonNull(clazz)))
                .<RegisterMapper<D, A>>map(Map.Entry::getValue).findAny();
    }

    public <D, A extends GenericRecord> RegisterMapper<D, A> register(Class<?> domainClazz, Class<?> avroClazz,
                                                                      Function<D, A> fromDomain,
                                                                      Function<A, D> toDomain) {
        RegisterMapper<D, A> registerMapper = new RegisterMapper<>(requireNonNull(fromDomain), requireNonNull(toDomain));
        mappers.put(new RegisterKey(requireNonNull(domainClazz), requireNonNull(avroClazz)),
                registerMapper);
        return registerMapper;
    }

    @Value
    private class RegisterKey {
        final Class<?> domainClass;
        final Class<?> serializedClass;
    }

    @Value
    static final class RegisterMapper<D, A> {
        final Function<D, A> fromDomainFunc;
        final Function<A, D> toDomainFunc;
    }

    private Predicate<Map.Entry<RegisterKey,RegisterMapper>> byClass(Class clazz) {
        return e -> e.getKey().domainClass().equals(clazz) || e.getKey().serializedClass().equals(clazz);
    }
}
