package com.davidosantos.kafka.training.kafkatraining;

import java.util.Set;
import java.util.logging.Logger;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;

public class MyProcessor implements Processor<String, String, Void, Void> {

    Logger logger = Logger.getLogger(this.toString());

    @Override
    public void process(Record<String, String> record) {
        logger.info("Hello data from topology, key: " +record.key() + " value: " + record.value());
        
    }

    
    
}
