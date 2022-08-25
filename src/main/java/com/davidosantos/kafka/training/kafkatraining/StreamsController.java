package com.davidosantos.kafka.training.kafkatraining;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import io.swagger.v3.oas.annotations.parameters.RequestBody;

@Controller
public class StreamsController {


    @Autowired    
    KafkaTrainingApplication application;
   
    @RequestMapping(value = "/{key}", method = RequestMethod.GET)
    ResponseEntity<JoinedObjects> getData(@PathVariable String key){
        
        InitializerForAggregate initializerForAggregate = (InitializerForAggregate) application.getConsumer().store(
            StoreQueryParameters.fromNameAndType("state-store-for-InitializerForAggregate", QueryableStoreTypes.keyValueStore())
        ).get(key);
        return ResponseEntity.ok(initializerForAggregate.getAggregateMap().get(key));
        
    }
    
}
