package io.simplesource.kafka.serialization.avro.mappers;

import io.simplesource.kafka.serialization.avro.mappers.domain.UserAccountDomain;
import io.simplesource.kafka.serialization.test.wire.UserAccount;
import org.apache.avro.generic.GenericRecord;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class GenericMapperRegistryTest {

    private DomainMapperRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new DomainMapperRegistry();
    }

    @Test
    void registerShouldOverrideExistingEntryWhenRegisterSameClassesAgain() {
        Function<UserAccountDomain, UserAccount> toSerialized = (UserAccountDomain d) -> new UserAccount();
        Function<UserAccountDomain, UserAccount> toSerialized2 = (UserAccountDomain d) -> new UserAccount();
        Function<UserAccount, UserAccountDomain> fromSerialized = (UserAccount d) -> new UserAccountDomain("Sarah Jones", BigDecimal.ZERO);
        Function<UserAccount, UserAccountDomain> fromSerialized2 = (UserAccount d) -> new UserAccountDomain("Sarah Jones 2", BigDecimal.ONE);
        Class<UserAccountDomain> domainClass = UserAccountDomain.class;
        Class<UserAccount> serializedClass = UserAccount.class;

        registry.register(domainClass, serializedClass, toSerialized, fromSerialized);
        registry.register(domainClass, serializedClass, toSerialized2, fromSerialized2);

        DomainMapperRegistry.RegistryMapper<UserAccountDomain, UserAccount> mapperFunctions =
                new DomainMapperRegistry.RegistryMapper<>(toSerialized2, fromSerialized2);
        assertThat(registry.<UserAccountDomain, UserAccount>mapperFor(domainClass)).isPresent().contains(mapperFunctions);
    }

    @Test
    void mapperForShouldReturnEmptyWhenSearchForNotRegisteredClasses() {
        Optional<DomainMapperRegistry.RegistryMapper<UserAccountDomain, GenericRecord>> actualResult = registry.mapperFor(UserAccountDomain.class);

        Assertions.assertThat(actualResult).isEmpty();
    }

    @Test
    void mapperForShouldReturnSameMapperFunctionsForRegisteredDomainAndSerializedPairs() {
        Function<UserAccountDomain, UserAccount> toSerialized = (UserAccountDomain d) -> new UserAccount();
        Function<UserAccount, UserAccountDomain> fromSerialized = (UserAccount d) -> new UserAccountDomain("Sarah Jones", BigDecimal.ZERO);
        Class<UserAccountDomain> domainClass = UserAccountDomain.class;
        Class<UserAccount> serializedClass = UserAccount.class;
        registry.register(domainClass, serializedClass, toSerialized, fromSerialized);

        Optional<DomainMapperRegistry.RegistryMapper<UserAccountDomain, UserAccount>> actualResultForDomainClass = registry.mapperFor(domainClass);
        Optional<DomainMapperRegistry.RegistryMapper<UserAccountDomain, UserAccount>> actualResultForSerializedClass = registry.mapperFor(serializedClass);

        DomainMapperRegistry.RegistryMapper<UserAccountDomain, UserAccount> mapperFunctions =
                new DomainMapperRegistry.RegistryMapper<>(toSerialized, fromSerialized);
        Assertions.assertThat(actualResultForDomainClass).isPresent().contains(mapperFunctions);
        Assertions.assertThat(actualResultForSerializedClass).isPresent().contains(mapperFunctions);
    }

    @Test
    void mapperForShouldDoExactMatchOfClassWhenSearchForRegisteredMappers() {
        Function<ParentDomainModel, UserAccount> toSerialized = (ParentDomainModel d) -> new UserAccount();
        Function<UserAccount, ParentDomainModel> fromSerialized = (UserAccount d) -> new ParentDomainModel();
        registry.register(ParentDomainModel.class, UserAccount.class, toSerialized, fromSerialized);

        Optional<DomainMapperRegistry.RegistryMapper<ParentDomainModel, UserAccount>> actualResult = registry.mapperFor(ChildUserAccountDomain.class);

        Assertions.assertThat(actualResult).isEmpty();
    }

    private static class ParentDomainModel {}
    private static class ChildUserAccountDomain extends ParentDomainModel {}
}