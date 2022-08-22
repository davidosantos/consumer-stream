package com.davidosantos.kafka.training.kafkatraining;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class CustomSerdes implements Serde<SimplePojoObject> {

    @Override
    public Serializer<SimplePojoObject> serializer() {
        return new CustomSimpleSerializer();
    }

    @Override
    public Deserializer<SimplePojoObject> deserializer() {
        return new CustomeSimpleDeserializer();
    }
    
}
