package com.davidosantos.kafka.training.kafkatraining;

import java.util.Date;

public class SimplePojoObject implements MessageTimingInterface {
    private String name;
    private int age;
    private Date birthDate;
    private String timestamp;
   
   
    // Getter Methods 
   
    public String getName() {
     return name;
    }
   
    public int getAge() {
     return age;
    }
   
    public Date getBirthDate() {
     return birthDate;
    }
   
    // Setter Methods 
   
    public void setName(String name) {
     this.name = name;
    }
   
    public void setAge(int age) {
     this.age = age;
    }
   
    public void setBirthDate(Date birthDate) {
     this.birthDate = birthDate;
    }


    @Override
    public String toString() {
        return "{" +
            " name='" + getName() + "'" +
            ", age='" + getAge() + "'" +
            ", birthDate='" + getBirthDate() + "'" +
            ", timestamp='" + getTimestamp() + "'" +
            "}";
    }
   

    @Override
    public String getMessageTiming() {
        return getTimestamp();
    }

    public String getTimestamp() {
        return this.timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }


}
