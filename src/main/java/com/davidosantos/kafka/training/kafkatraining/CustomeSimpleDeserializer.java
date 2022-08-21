package com.davidosantos.kafka.training.kafkatraining;

import org.apache.kafka.common.serialization.Deserializer;

public class CustomeSimpleDeserializer implements Deserializer<SimplePojoObject> {

    @Override
    public SimplePojoObject deserialize(String topic, byte[] data) {
        // TODO Auto-generated method stub
        return null;
    }
    
}
