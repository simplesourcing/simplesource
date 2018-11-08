package io.simplesource.kafka.serialization.avro.mappers;

import io.simplesource.kafka.serialization.avro.mappers.domain.Money;
import io.simplesource.kafka.serialization.avro.mappers.domain.UserAccountDomain;
import io.simplesource.kafka.serialization.avro.generated.UserAccount;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class DomainSerializedClassMapperImplTest {

    private DomainSerializedClassMapper<UserAccountDomain, UserAccount> domainSerializedClassMapper;

    @Mock
    private DomainMapperBuilder domainMapperBuilder;
    @Mock
    private DomainMapperRegistry domainMapperRegistry;

    @BeforeEach
    void setUp() {
        domainSerializedClassMapper = new DomainSerializedClassMapperImpl<>(domainMapperBuilder,
                domainMapperRegistry);
    }

    @Test
    void mapperForShouldThrowExceptionWhenOneOfArgumentIsNull() {
        assertThrows(NullPointerException.class, () ->
                domainSerializedClassMapper.mapperFor(UserAccountDomain.class, null)
        );
    }

    @Test
    void toSerializedShouldThrowExceptionWhenFunctionIsNull() {
        assertThrows(NullPointerException.class, () -> {
            domainSerializedClassMapper.mapperFor(UserAccountDomain.class, UserAccount.class)
                    .toSerialized(null);
        });
    }

    @Test
    void fromSerializedShouldThrowExceptionWhenFunctionIsNull() {
        assertThrows(NullPointerException.class, () -> {
            domainSerializedClassMapper.mapperFor(UserAccountDomain.class, UserAccount.class)
                    .toSerialized((UserAccountDomain d) -> new UserAccount())
                    .fromSerialized(null);
        });
    }

    @Test
    void registerShouldAddMapperAndClassesToRegistry() {
        Function<UserAccountDomain, UserAccount> toSerialized = (UserAccountDomain d) -> new UserAccount();
        Function<UserAccount, UserAccountDomain> fromSerialized = (UserAccount d) -> new UserAccountDomain("Sarah Jones", Money.ZERO);
        Class<UserAccountDomain> domainClass = UserAccountDomain.class;
        Class<UserAccount> serializedClass = UserAccount.class;

        domainSerializedClassMapper.mapperFor(domainClass, serializedClass)
                .toSerialized(toSerialized)
                .fromSerialized(fromSerialized)
                .register();

        verify(domainMapperRegistry).register(domainClass, serializedClass, toSerialized, fromSerialized);
    }

    @Test
    void buildShouldReturnSameDomainMapperBuilderPassedWhileInitialize() {
        Function<UserAccountDomain, UserAccount> toSerialized = (UserAccountDomain d) -> new UserAccount();
        Function<UserAccount, UserAccountDomain> fromSerialized = (UserAccount d) -> null;

        Class<UserAccountDomain> domainClass = UserAccountDomain.class;
        Class<UserAccount> serializedClass = UserAccount.class;

        DomainMapperBuilder mapperBuilder = domainSerializedClassMapper.mapperFor(domainClass, serializedClass)
                .toSerialized(toSerialized)
                .fromSerialized(fromSerialized)
                .register();

        assertThat(mapperBuilder).isNotNull().isEqualTo(domainMapperBuilder);
    }
}