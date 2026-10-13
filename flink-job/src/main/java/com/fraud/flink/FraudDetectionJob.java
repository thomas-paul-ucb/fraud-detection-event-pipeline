package com.fraud.flink;

import com.fraud.flink.functions.GeoImpossibilityFunction;
import com.fraud.flink.model.FraudAlert;
import com.fraud.flink.model.Transaction;
import com.fraud.flink.serde.TransactionDeserializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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

        DataStream<Transaction> transactions = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-transactions");

        DataStream<FraudAlert> geoAlerts = transactions
                .keyBy(tx -> tx.userId)
                .process(new GeoImpossibilityFunction(900, 50, Duration.ofHours(24)))
                .name("geo-impossibility");

        geoAlerts.print();

        env.execute("Fraud Detection - Flink Stream Processor");
    }
}