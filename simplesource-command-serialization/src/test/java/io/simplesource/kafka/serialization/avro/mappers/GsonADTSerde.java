package io.simplesource.kafka.serialization.avro.mappers;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.Value;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

class GsonADTSerde {

    @Value(staticConstructor = "of")
    static class WithClass {
        String cls;
        String cmd;
    }

    static class GsonSerializer<T> implements Serializer<T> {

        private Gson gson = new GsonBuilder().create();

        @Override
        public void configure(Map<String, ?> config, boolean isKey) {
        }

        @Override
        public byte[] serialize(String s, T t) {
            WithClass wt = WithClass.of(t.getClass().getName(), gson.toJson(t));
            return gson.toJson(wt).getBytes();
        }

        @Override
        public void close() {
            // this is called right before destruction
        }
    }

    static class GsonDeserializer<T> implements Deserializer<T> {
        private Gson gson = new GsonBuilder().create();

        GsonDeserializer() {
        }

        @Override
        public void configure(Map<String, ?> config, boolean isKey) {
        }

        @Override
        public T deserialize(String topic, byte[] bytes) {
            WithClass wc = gson.fromJson(new String(bytes), WithClass.class);
            Class<T> cls;
            try {
                cls = (Class<T>) Class.forName(wc.cls());
            } catch (Exception e){
                throw new RuntimeException("Deserialization exception", e);
            }
            T t = gson.fromJson(wc.cmd(), cls);
            return t;
        }

        @Override
        public void close() {
        }
    }

    static public <T> Serde<T> of(Class<T> cls) {
        return Serdes.serdeFrom(new GsonSerializer<>(), new GsonDeserializer<>());
    }
}
