package com.kafka.aws.models;

import lombok.Data;

@Data
public class Transaction {
    private String name;
    private String lastname;
    private String username;
    private Double amount;
}
