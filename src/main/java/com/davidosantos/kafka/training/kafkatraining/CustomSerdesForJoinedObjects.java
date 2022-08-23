package com.davidosantos.kafka.training.kafkatraining;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class CustomSerdesForJoinedObjects implements Serde<JoinedObjects> {

    @Override
    public Serializer<JoinedObjects> serializer() {
        return new JoinedObjectsSerializer();
    }

    @Override
    public Deserializer<JoinedObjects> deserializer() {
        return new JoinedObjectsDeserializer();
    }
    
}
