package com.walmart.spark.streaming.poc.domain;

import lombok.Data;
import java.util.List;

@Data
public class PersonInfo {
    private String name;
    private String job;
    private String city;
    private String state;
    private String dob;
    private List<Appointment> appointments;
}
