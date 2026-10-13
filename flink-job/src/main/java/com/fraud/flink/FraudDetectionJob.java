package com.fraud.flink;

import com.fraud.flink.functions.CountAggregate;
import com.fraud.flink.functions.GeoImpossibilityFunction;
import com.fraud.flink.functions.VelocityAlertFunction;
import com.fraud.flink.model.FraudAlert;
import com.fraud.flink.model.Transaction;
import com.fraud.flink.serde.TransactionDeserializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;

import java.time.Duration;

public class FraudDetectionJob {

    public static void main(String[] args) throws Exception {
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_URL", "kafka:29092");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<Transaction> source = KafkaSource.<Transaction>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics("pending-transactions")
                .setGroupId("flink-fraud-detector")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new TransactionDeserializer())
                .build();

        // Event time = the transaction's own timestamp, not when Flink sees it.
        // Tolerate events arriving up to 5s out of order.
        WatermarkStrategy<Transaction> watermarks = WatermarkStrategy
                .<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((tx, kafkaTs) -> tx.eventTimeMillis)
                .withIdleness(Duration.ofMinutes(1));

        DataStream<Transaction> transactions = env
                .fromSource(source, watermarks, "kafka-transactions");

        // Velocity: more than 5 transactions in any 60s window, checked every 10s
        DataStream<FraudAlert> velocityAlerts = transactions
                .keyBy(tx -> tx.userId)
                .window(SlidingEventTimeWindows.of(Duration.ofSeconds(60), Duration.ofSeconds(10)))
                .aggregate(new CountAggregate(), new VelocityAlertFunction(5))
                .name("velocity-sliding-window");

        // Geo impossibility: faster than 900 km/h between consecutive transactions
        DataStream<FraudAlert> geoAlerts = transactions
                .keyBy(tx -> tx.userId)
                .process(new GeoImpossibilityFunction(900, 50, Duration.ofHours(24)))
                .name("geo-impossibility");

        velocityAlerts.union(geoAlerts).print();

        env.execute("Fraud Detection - Flink Stream Processor");
    }
}