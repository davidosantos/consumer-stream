package com.davidosantos.kafka.training.kafkatraining;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.serialization.Serializer;

import com.google.gson.Gson;

public class JoinedObjectsSerializer implements Serializer<JoinedObjects> {

    private Gson gson = new Gson();

    @Override
    public byte[] serialize(String topic, JoinedObjects data) {
        if (data == null) return null;
        return gson.toJson(data).getBytes(StandardCharsets.UTF_8);
    }
    
}
