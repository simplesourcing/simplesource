package io.simplesource.kafka.serialization.avro.mappers;

import io.simplesource.kafka.serialization.avro.mappers.DomainMapperRegistry.RegisterMapper;
import io.simplesource.kafka.serialization.avro.mappers.domain.UserAccountDomainCommand.CreateAccount;
import io.simplesource.kafka.serialization.test.wire.CreateUserAccount;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class DomainMapperRegistryTest {
    private static final Function<CreateAccount, CreateUserAccount> FROM_CREATE_ACCOUNT_DOMAIN_FUNC =
            v -> new CreateUserAccount(v.name(), v.balance());
    private static final Function<CreateUserAccount, CreateAccount> TO_CREATE_ACCOUNT_DOMAIN_FUNC =
            v -> new CreateAccount(v.getUsername(), v.getBalance());

    private DomainMapperRegistry target;

    @BeforeEach
    void setUp() {
        target = new DomainMapperRegistry();
    }

    @Test
    void mapperForShouldReturnEmptyWhenSearchForDomainClassThatWasNotRegisteredBefore() {
        Optional<RegisterMapper<CreateAccount, CreateUserAccount>> actualResult = target.mapperFor(CreateAccount.class);

        assertThat(actualResult).isEmpty();
    }

    @Test
    void registerShouldAddDomainAndSerializedClassesToRegistry() {
        Class<?> domainClass = CreateAccount.class;
        Class<?> serializedClass = CreateUserAccount.class;

        RegisterMapper<CreateAccount, CreateUserAccount> registerMapper = target.register(domainClass, serializedClass, FROM_CREATE_ACCOUNT_DOMAIN_FUNC,
                TO_CREATE_ACCOUNT_DOMAIN_FUNC);

        assertThat(registerMapper.fromDomainFunc()).isSameAs(FROM_CREATE_ACCOUNT_DOMAIN_FUNC);
        assertThat(registerMapper.toDomainFunc()).isSameAs(TO_CREATE_ACCOUNT_DOMAIN_FUNC);
    }

    @Test
    void registerShouldOverridePreviouslyRegisteredDomainAndSerializedClasses() {
        Class<?> domainClass = CreateAccount.class;
        Class<?> serializedClass = CreateUserAccount.class;
        Function<CreateAccount, CreateUserAccount> fromDomain = mock(Function.class);
        Function<CreateUserAccount, CreateAccount> toDomain = mock(Function.class);

        RegisterMapper<CreateAccount, CreateUserAccount> registerMapper1 = target.register(domainClass, serializedClass,
                fromDomain, toDomain);
        RegisterMapper<CreateAccount, CreateUserAccount> registerMapper2 = target.register(domainClass, serializedClass,
                FROM_CREATE_ACCOUNT_DOMAIN_FUNC, TO_CREATE_ACCOUNT_DOMAIN_FUNC);

        assertThat(registerMapper2.fromDomainFunc()).isNotSameAs(registerMapper1.fromDomainFunc());
        assertThat(registerMapper2.toDomainFunc()).isNotSameAs(registerMapper1.toDomainFunc());
    }

    @Test
    void givenDomainAndSerializedClassesAreRegisteredMapperForShouldReturnRegisteredMappingFunctions() {
        Class<?> domainClass = CreateAccount.class;
        Class<?> serializedClass = CreateUserAccount.class;
        target.register(domainClass, serializedClass, FROM_CREATE_ACCOUNT_DOMAIN_FUNC, TO_CREATE_ACCOUNT_DOMAIN_FUNC);

        assertThat(target.<CreateAccount, CreateUserAccount>mapperFor(domainClass))
                .contains(new RegisterMapper<>(FROM_CREATE_ACCOUNT_DOMAIN_FUNC, TO_CREATE_ACCOUNT_DOMAIN_FUNC));
        assertThat(target.<CreateAccount, CreateUserAccount>mapperFor(serializedClass))
                .contains(new RegisterMapper<>(FROM_CREATE_ACCOUNT_DOMAIN_FUNC, TO_CREATE_ACCOUNT_DOMAIN_FUNC));
    }
}