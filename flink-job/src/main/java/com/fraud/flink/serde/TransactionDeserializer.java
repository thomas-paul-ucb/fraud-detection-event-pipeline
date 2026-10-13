package com.fraud.flink.serde;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import com.fraud.flink.model.Transaction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;

/**
 * Parses the JSON the FastAPI service writes to Kafka.
 * Bad records are logged and skipped so one poison message can't crash the job.
 */
public class TransactionDeserializer implements DeserializationSchema<Transaction> {
    private static final Logger LOG = LoggerFactory.getLogger(TransactionDeserializer.class);
    private transient ObjectMapper mapper;

    @Override
    public Transaction deserialize(byte[] message) throws IOException {
        if (mapper == null) mapper = new ObjectMapper();
        JsonNode n = mapper.readTree(message);
        if (!n.hasNonNull("user_id") || !n.hasNonNull("amount")) return null;

        Transaction tx = new Transaction();
        tx.transactionId = n.path("transaction_id").asText(null);
        tx.userId = n.get("user_id").asText();
        tx.amount = n.get("amount").asDouble();
        tx.merchant = n.path("merchant").asText(null);
        tx.lat = n.hasNonNull("lat") ? n.get("lat").asDouble() : null;
        tx.lon = n.hasNonNull("lon") ? n.get("lon").asDouble() : null;
        tx.eventTimeMillis = n.hasNonNull("timestamp")
                ? parseTimestamp(n.get("timestamp").asText())
                : System.currentTimeMillis();
        return tx;
    }

    @Override
    public void deserialize(byte[] message, Collector<Transaction> out) {
        try {
            Transaction tx = deserialize(message);
            if (tx != null) out.collect(tx);
        } catch (Exception e) {
            LOG.warn("Skipping malformed transaction record: {}", e.getMessage());
        }
    }

    /** Python isoformat() gives either "...+00:00" or a naive UTC string with no offset. */
    static long parseTimestamp(String ts) {
        try {
            return OffsetDateTime.parse(ts).toInstant().toEpochMilli();
        } catch (DateTimeParseException e) {
            return LocalDateTime.parse(ts).toInstant(ZoneOffset.UTC).toEpochMilli();
        }
    }

    @Override
    public boolean isEndOfStream(Transaction next) {
        return false;
    }

    @Override
    public TypeInformation<Transaction> getProducedType() {
        return TypeInformation.of(Transaction.class);
    }
}