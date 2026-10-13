package com.fraud.flink.model;

/**
 * Transaction event read from the `pending-transactions` Kafka topic.
 * Public fields + no-arg constructor so Flink treats it as a POJO.
 */
public class Transaction {
    public String transactionId;
    public String userId;
    public double amount;
    public String merchant;
    public Double lat;
    public Double lon;
    /** Event time in epoch millis, parsed from the API's ISO-8601 timestamp. */
    public long eventTimeMillis;

    public Transaction() {}

    public boolean hasLocation() {
        return lat != null && lon != null;
    }

    @Override
    public String toString() {
        return "Transaction{" + transactionId + ", user=" + userId + ", amount=" + amount
                + ", lat=" + lat + ", lon=" + lon + ", t=" + eventTimeMillis + "}";
    }
}