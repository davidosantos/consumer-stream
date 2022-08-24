package com.davidosantos.kafka.training.kafkatraining;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.google.gson.Gson;

public class CustomSerDesForInitializerForAggregate implements Serde<InitializerForAggregate> {

    @Override
    public Serializer<InitializerForAggregate> serializer() {
        
        return new SerializerForAggregate();
    }

    @Override
    public Deserializer<InitializerForAggregate> deserializer() {
        
        return new DeserializerForAggregate();
    }

    class SerializerForAggregate implements Serializer<InitializerForAggregate> {

        Gson gson = new Gson();

        @Override
        public byte[] serialize(String topic, InitializerForAggregate data) {
            if (data == null)
                return null;
            return gson.toJson(data).getBytes(StandardCharsets.UTF_8);
        }
    }

    class DeserializerForAggregate implements Deserializer<InitializerForAggregate> {

        Gson gson = new Gson();

        @Override
        public InitializerForAggregate deserialize(String topic, byte[] data) {
            if (data == null)
                return null;
            return gson.fromJson(new String(data, StandardCharsets.UTF_8), InitializerForAggregate.class);
        }
    }
}
