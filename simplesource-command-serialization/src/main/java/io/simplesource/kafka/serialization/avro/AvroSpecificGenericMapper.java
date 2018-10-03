package io.simplesource.kafka.serialization.avro;

import io.simplesource.kafka.serialization.util.GenericMapper;
import org.apache.avro.Conversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;

import static java.util.Objects.isNull;

public class AvroSpecificGenericMapper<D extends GenericRecord> implements GenericMapper<D, GenericRecord> {

    private AvroSpecificGenericMapper() {
    }

    @Override
    public GenericRecord toGeneric(final D value) {
        return value;
    }

    @Override
    public D fromGeneric(final GenericRecord serialized) {
        GenericData.get().addLogicalTypeConversion(new Conversions.DecimalConversion());
        SpecificData specificData = SpecificData.get();
        specificData.addLogicalTypeConversion(new Conversions.DecimalConversion());
        return isNull(serialized) ? null : (D) specificData.deepCopy(serialized.getSchema(), serialized);
    }

    private static final AvroSpecificGenericMapper INSTANCE = new AvroSpecificGenericMapper();

    public static <D extends GenericRecord> GenericMapper<D, GenericRecord> specificDomainMapper() {
        return INSTANCE;
    }
}
