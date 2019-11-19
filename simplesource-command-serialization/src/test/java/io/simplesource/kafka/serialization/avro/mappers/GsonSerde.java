package io.simplesource.kafka.serialization.avro.mappers;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import lombok.Value;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

class GsonSerde {
    static class GsonSerializer<T> implements Serializer<T> {

        private Gson gson = new GsonBuilder().create();

        @Override
        public void configure(Map<String, ?> config, boolean isKey) {
        }

        @Override
        public byte[] serialize(String s, T t) {
            return gson.toJson(t).getBytes();
        }

        @Override
        public void close() {
            // this is called right before destruction
        }
    }

    static class GsonDeserializer<T> implements Deserializer<T> {
        public static final String CONFIG_VALUE_CLASS = "value.deserializer.class";
        public static final String CONFIG_KEY_CLASS = "key.deserializer.class";
        private Class<T> cls;
        private Gson gson = new GsonBuilder().create();

        GsonDeserializer(Class<T> cls) {
            this.cls = cls;
        }

        @Override
        public void configure(Map<String, ?> config, boolean isKey) {
        }

        @Override
        public T deserialize(String topic, byte[] bytes) {
            return (T) gson.fromJson(new String(bytes), cls);
        }

        @Override
        public void close() {
        }
    }

    static public <T> Serde<T> of(Class<T> cls) {
        return Serdes.serdeFrom(new GsonSerde.GsonSerializer<T>(), new GsonSerde.GsonDeserializer<T>(cls));
    }
}
