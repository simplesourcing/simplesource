package io.simplesource.kafka.serialization.avro.mappers;

import io.simplesource.kafka.serialization.avro.mappers.DomainMapperRegistry.RegisterMapper;
import io.simplesource.kafka.serialization.avro.mappers.domain.Money;
import io.simplesource.kafka.serialization.avro.mappers.domain.UserAccountDomainCommand;
import io.simplesource.kafka.serialization.avro.generated.CreateAccount;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class DomainMapperRegistryTest {
    private static final Function<UserAccountDomainCommand.CreateAccount, CreateAccount> FROM_CREATE_ACCOUNT_DOMAIN_FUNC =
            v -> new CreateAccount(v.name(), v.balance().getAmount());
    private static final Function<CreateAccount, UserAccountDomainCommand.CreateAccount> TO_CREATE_ACCOUNT_DOMAIN_FUNC =
            v -> new UserAccountDomainCommand.CreateAccount(v.getName(), Money.valueOf(v.getBalance()));

    private DomainMapperRegistry target;

    @BeforeEach
    void setUp() {
        target = new DomainMapperRegistry();
    }

    @Test
    void mapperForShouldReturnEmptyWhenSearchForDomainClassThatWasNotRegisteredBefore() {
        Optional<RegisterMapper<CreateAccount, CreateAccount>> actualResult = target.mapperFor(CreateAccount.class);

        assertThat(actualResult).isEmpty();
    }

    @Test
    void registerShouldAddDomainAndSerializedClassesToRegistry() {
        Class<?> domainClass = UserAccountDomainCommand.CreateAccount.class;
        Class<?> serializedClass = CreateAccount.class;

        RegisterMapper<UserAccountDomainCommand.CreateAccount, CreateAccount> registerMapper = target.register(domainClass, serializedClass, FROM_CREATE_ACCOUNT_DOMAIN_FUNC,
                TO_CREATE_ACCOUNT_DOMAIN_FUNC);

        assertThat(registerMapper.fromDomainFunc()).isSameAs(FROM_CREATE_ACCOUNT_DOMAIN_FUNC);
        assertThat(registerMapper.toDomainFunc()).isSameAs(TO_CREATE_ACCOUNT_DOMAIN_FUNC);
    }

    @Test
    void registerShouldOverridePreviouslyRegisteredDomainAndSerializedClasses() {
        Class<?> domainClass = UserAccountDomainCommand.CreateAccount.class;
        Class<?> serializedClass = CreateAccount.class;
        Function<UserAccountDomainCommand.CreateAccount, CreateAccount> fromDomain = mock(Function.class);
        Function<CreateAccount, UserAccountDomainCommand.CreateAccount> toDomain = mock(Function.class);

        RegisterMapper<UserAccountDomainCommand.CreateAccount, CreateAccount> registerMapper1 = target.register(domainClass, serializedClass,
                fromDomain, toDomain);
        RegisterMapper<UserAccountDomainCommand.CreateAccount, CreateAccount> registerMapper2 = target.register(domainClass, serializedClass,
                FROM_CREATE_ACCOUNT_DOMAIN_FUNC, TO_CREATE_ACCOUNT_DOMAIN_FUNC);

        assertThat(registerMapper2.fromDomainFunc()).isNotSameAs(registerMapper1.fromDomainFunc());
        assertThat(registerMapper2.toDomainFunc()).isNotSameAs(registerMapper1.toDomainFunc());
    }

    @Test
    void givenDomainAndSerializedClassesAreRegisteredMapperForShouldReturnRegisteredMappingFunctions() {
        Class<?> domainClass = UserAccountDomainCommand.CreateAccount.class;
        Class<?> serializedClass = CreateAccount.class;
        target.register(domainClass, serializedClass, FROM_CREATE_ACCOUNT_DOMAIN_FUNC, TO_CREATE_ACCOUNT_DOMAIN_FUNC);

        assertThat(target.<UserAccountDomainCommand.CreateAccount, CreateAccount>mapperFor(domainClass))
                .contains(new RegisterMapper<>(FROM_CREATE_ACCOUNT_DOMAIN_FUNC, TO_CREATE_ACCOUNT_DOMAIN_FUNC));
        assertThat(target.<UserAccountDomainCommand.CreateAccount, CreateAccount>mapperFor(serializedClass))
                .contains(new RegisterMapper<>(FROM_CREATE_ACCOUNT_DOMAIN_FUNC, TO_CREATE_ACCOUNT_DOMAIN_FUNC));
    }
}