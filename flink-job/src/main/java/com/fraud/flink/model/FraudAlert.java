package com.fraud.flink.model;

/** Alert emitted by the fraud rules. */
public class FraudAlert {
    public static final String VELOCITY = "velocity";
    public static final String GEO_IMPOSSIBILITY = "geo_impossibility";

    public String userId;
    public String type;
    /** Event time the alert refers to. */
    public long eventTimeMillis;
    /** Rule-specific number: tx count for velocity, implied km/h for geo. */
    public double value;
    public String detail;

    public FraudAlert() {}

    public FraudAlert(String userId, String type, long eventTimeMillis, double value, String detail) {
        this.userId = userId;
        this.type = type;
        this.eventTimeMillis = eventTimeMillis;
        this.value = value;
        this.detail = detail;
    }

    @Override
    public String toString() {
        return "FraudAlert{" + type + ", user=" + userId + ", value=" + value + ", " + detail + "}";
    }
}