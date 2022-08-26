package com.davidosantos.kafka.training.kafkatraining;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.google.gson.Gson;

public class SerDesForSimplePojoObject implements Serde<SimplePojoObject> {

    @Override
    public Serializer<SimplePojoObject> serializer() {

        return new SerializerForSimplePojoObject();
    }

    @Override
    public Deserializer<SimplePojoObject> deserializer() {

        return new DeserializerForSimplePojoObject();
    }

    class SerializerForSimplePojoObject
            implements Serializer<SimplePojoObject> {

        Gson gson = new Gson();

        @Override
        public byte[] serialize(String topic, SimplePojoObject data) {
            if (data == null)
                return null;
            return gson.toJson(data).getBytes(StandardCharsets.UTF_8);
        }
    }

    class DeserializerForSimplePojoObject
            implements Deserializer<SimplePojoObject> {

        Gson gson = new Gson();

        @Override
        public SimplePojoObject deserialize(String topic, byte[] data) {
            if (data == null)
                return null;
            return gson.fromJson(new String(data, StandardCharsets.UTF_8), SimplePojoObject.class);
        }
    }
}
