package com.davidosantos.kafka.training.kafkatraining;

public class JoinedObjects {
    SimplePojoObject key1;
    SimplePojoObject key2;

    public JoinedObjects(SimplePojoObject left, SimplePojoObject right) {
        this.key1 = left;
        this.key2 = right;
    }


    public SimplePojoObject getKey1() {
        return this.key1;
    }

    public void setKey1(SimplePojoObject key1) {
        this.key1 = key1;
    }

    public SimplePojoObject getKey2() {
        return this.key2;
    }

    public void setKey2(SimplePojoObject key2) {
        this.key2 = key2;
    }

    @Override
    public String toString() {
        return "{" +
            " key1='" + getKey1() + "'" +
            ", key2='" + getKey2() + "'" +
            "}";
    }
 
}