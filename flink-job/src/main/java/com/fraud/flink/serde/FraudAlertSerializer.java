package com.fraud.flink.serde;

import com.fraud.flink.model.FraudAlert;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Instant;

/** Serializes alerts to JSON (snake_case to match the Python side). */
public class FraudAlertSerializer implements SerializationSchema<FraudAlert> {
    private transient ObjectMapper mapper;

    @Override
    public byte[] serialize(FraudAlert a) {
        if (mapper == null) mapper = new ObjectMapper();
        ObjectNode n = mapper.createObjectNode();
        n.put("user_id", a.userId);
        n.put("type", a.type);
        n.put("event_time", Instant.ofEpochMilli(a.eventTimeMillis).toString());
        n.put("value", a.value);
        n.put("detail", a.detail);
        try {
            return mapper.writeValueAsBytes(n);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize alert " + a, e);
        }
    }
}