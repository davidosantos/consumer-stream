package com.davidosantos.kafka.training.kafkatraining;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.serialization.Deserializer;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class JoinedObjectsDeserializer implements Deserializer<JoinedObjects> {

    private Gson gson = new Gson();

    @Override
    public JoinedObjects deserialize(String topic, byte[] data) {
        if( data == null) return null;
        return gson.fromJson(new String(data,StandardCharsets.UTF_8), JoinedObjects.class);
    }
    
}
