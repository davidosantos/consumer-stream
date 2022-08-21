package com.davidosantos.kafka.training.kafkatraining;

import java.util.Date;

public class SimplePojoObject {
    private String name;
    private int age;
    private Date birthDate;
   
   
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


}
