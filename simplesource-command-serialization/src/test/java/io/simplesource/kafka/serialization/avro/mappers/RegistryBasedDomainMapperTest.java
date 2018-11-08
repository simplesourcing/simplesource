package io.simplesource.kafka.serialization.avro.mappers;

import io.simplesource.kafka.serialization.avro.AvroSpecificGenericMapper;
import io.simplesource.kafka.serialization.avro.mappers.domain.Money;
import io.simplesource.kafka.serialization.avro.mappers.domain.UserAccountDomainCommand;
import io.simplesource.kafka.serialization.avro.mappers.domain.UserAccountDomainCommand.UpdateUserName;
import io.simplesource.kafka.serialization.avro.generated.CreateAccount;
import io.simplesource.kafka.serialization.util.GenericMapper;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Optional;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RegistryBasedDomainMapperTest {

    private static final Money BALANCE = Money.ZERO;
    private static final String USERNAME = "Sarah Jones";

    @Mock
    private DomainMapperRegistry registry;
    private RegistryBasedDomainMapper<UserAccountDomainCommand, GenericRecord> mapper;
    private GenericMapper<GenericRecord, GenericRecord> avroSpecificGenericMapper;

    @BeforeEach
    void setUp() {
        avroSpecificGenericMapper = spy(AvroSpecificGenericMapper.specificDomainMapper());
        mapper = new RegistryBasedDomainMapper<>(registry, avroSpecificGenericMapper, Object::getClass);
    }

    @Test
    void fromDomainShouldThrowDefaultExceptionWhenDomainObjHasTypeThatNotInTheRegistry() {
        UserAccountDomainCommand domainCommand = new UserAccountDomainCommand.CreateAccount(USERNAME, BALANCE);

        assertThrows(IllegalArgumentException.class, () -> mapper.toGeneric(domainCommand), "Class not supported");
    }

    @Test
    void toDomainShouldThrowDefaultExceptionWhenDomainObjHasTypeThatNotInTheRegistry() {
        CreateAccount serializedClass = new CreateAccount(USERNAME, BALANCE.getAmount());

        assertThrows(IllegalArgumentException.class, () -> mapper.fromGeneric(serializedClass), "Class not supported");
    }

    @Test
    void fromDomainShouldUseRegisteredMapperFunction() {
        UserAccountDomainCommand domainCommand = new UserAccountDomainCommand.CreateAccount(USERNAME, BALANCE);
        CreateAccount serializedClass = new CreateAccount(USERNAME, BALANCE.getAmount());
        givenDomainAndSerializedObjsInitializeRegistry(domainCommand, serializedClass);

        GenericRecord actualResult = mapper.toGeneric(domainCommand);

        assertThat(actualResult).isEqualTo(serializedClass);
    }

    @Test
    void toDomainShouldUseRegisteredMapperFunction() {
        UserAccountDomainCommand domainCommand = new UserAccountDomainCommand.CreateAccount(USERNAME, BALANCE);
        CreateAccount serializedClass = new CreateAccount(USERNAME, BALANCE.getAmount());
        givenDomainAndSerializedObjsInitializeRegistry(domainCommand, serializedClass);

        UserAccountDomainCommand actualResult = mapper.fromGeneric(serializedClass);

        assertThat(actualResult).isEqualTo(domainCommand);
    }

    @Test
    void toDomainShouldConvertSerializedObjToAvroSpecificRecordBeforeMappingToDomain() {
        UserAccountDomainCommand domainCommand = new UserAccountDomainCommand.CreateAccount(USERNAME, BALANCE);
        CreateAccount serializedCommand = new CreateAccount(USERNAME, BALANCE.getAmount());
        doReturn(serializedCommand).when(avroSpecificGenericMapper).fromGeneric(any(GenericRecord.class));
        when(registry.mapperFor(serializedCommand.getClass())).thenReturn(Optional.of(new DomainMapperRegistry.RegisterMapper<>(s -> serializedCommand, s -> domainCommand)));

        UserAccountDomainCommand actualResult = mapper.fromGeneric(serializedCommand);

        assertThat(actualResult).isEqualTo(domainCommand);
    }

    @Test
    void fromDomainShouldUseSuppliedExceptionSupplierWhenPassNotRegisteredClass() {
        UserAccountDomainCommand domainCommand = new UserAccountDomainCommand.CreateAccount(USERNAME, BALANCE);
        mapper.withExceptionSupplier(() -> new DomainMapperNotRegistered("Not registered domain mapper"));

        assertThrows(DomainMapperNotRegistered.class, () -> mapper.toGeneric(domainCommand),
                "Not registered domain mapper");
    }

    @Test
    void toDomainShouldUseSuppliedExceptionSupplierWhenPassNotRegisteredClass() {
        CreateAccount serializedCommand = new CreateAccount(USERNAME, BALANCE.getAmount());
        mapper.withExceptionSupplier(() -> new DomainMapperNotRegistered("Not registered domain mapper"));

        assertThrows(DomainMapperNotRegistered.class, () -> mapper.fromGeneric(serializedCommand),
                "Not registered domain mapper");
    }

    @Test
    void toDomainShouldUseDomainValueToClassTypeFunctionToLookupRegistry() {
        Class<?> domainValueType = UpdateUserName.class;
        Function<UserAccountDomainCommand, Class<?>> domainValueToType = (v) -> domainValueType;
        mapper = new RegistryBasedDomainMapper<>(registry, avroSpecificGenericMapper, domainValueToType);
        UserAccountDomainCommand domainCommand = new UserAccountDomainCommand.CreateAccount(USERNAME, BALANCE);
        CreateAccount serializedCommand = new CreateAccount(USERNAME, BALANCE.getAmount());
        DomainMapperRegistry.RegisterMapper<UserAccountDomainCommand, CreateAccount> registerMapper =
                new DomainMapperRegistry.RegisterMapper<>(s -> serializedCommand, s -> domainCommand);
        Mockito.lenient().doReturn(Optional.of(registerMapper)).when(registry).mapperFor(domainValueType);

        GenericRecord actualResult = mapper.toGeneric(domainCommand);

        assertThat(actualResult).isEqualTo(serializedCommand);
    }

    private void givenDomainAndSerializedObjsInitializeRegistry(UserAccountDomainCommand domainCommand,
                                                                CreateAccount serializedClass) {
        DomainMapperRegistry.RegisterMapper<UserAccountDomainCommand, CreateAccount> registerMapper =
                new DomainMapperRegistry.RegisterMapper<>(s -> serializedClass, s -> domainCommand);
        Mockito.lenient().doReturn(Optional.of(registerMapper)).when(registry).mapperFor(domainCommand.getClass());
        Mockito.lenient().doReturn(Optional.of(registerMapper)).when(registry).mapperFor(serializedClass.getClass());
    }

    private static class DomainMapperNotRegistered extends RuntimeException {
        DomainMapperNotRegistered(String message) {
            super(message);
        }
    }
}