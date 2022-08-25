package com.davidosantos.kafka.training.kafkatraining;

import java.util.HashMap;
import java.util.Map;

public class InitializerForAggregate{

    private Map<String,JoinedObjects> aggregateMap = new HashMap<String,JoinedObjects>();

    InitializerForAggregate addNewData(String currentKey, JoinedObjects joinedObjects) {
        if(aggregateMap.containsKey(currentKey)) {
            JoinedObjects joinedObjects2 = this.aggregateMap.get(currentKey);
            joinedObjects2.getKey1().setName(this.aggregateMap.get(currentKey).getKey1().getName()+"+");
            joinedObjects2.getKey1().setAge(this.aggregateMap.get(currentKey).getKey1().getAge()+1);
            this.aggregateMap.put(currentKey, joinedObjects2);
        }
        aggregateMap.put(currentKey, joinedObjects);
        return this;
    }


    @Override
    public String toString() {
        return "{" +
            " aggregateMap='" + this.aggregateMap + "'" +
            "}";
    }

    public Map<String,JoinedObjects> getAggregateMap() {
        return this.aggregateMap;
    }

    public void setAggregateMap(Map<String,JoinedObjects> aggregateMap) {
        this.aggregateMap = aggregateMap;
    }

    
}
