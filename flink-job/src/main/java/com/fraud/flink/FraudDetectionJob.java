package com.fraud.flink;

import com.fraud.flink.functions.CountAggregate;
import com.fraud.flink.functions.GeoImpossibilityFunction;
import com.fraud.flink.functions.VelocityAlertFunction;
import com.fraud.flink.model.FraudAlert;
import com.fraud.flink.model.Transaction;
import com.fraud.flink.serde.FraudAlertSerializer;
import com.fraud.flink.serde.TransactionDeserializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.time.Duration;

/**
 * FastAPI -> Kafka[pending-transactions] -> Flink -> Kafka[fraud-alerts]
 */
public class FraudDetectionJob {

    public static void main(String[] args) throws Exception {
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_URL", "kafka:29092");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Snapshot Kafka offsets + all keyed state every 10s; on failure, restore and replay
        env.enableCheckpointing(10_000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000);

        KafkaSource<Transaction> source = KafkaSource.<Transaction>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics("pending-transactions")
                .setGroupId("flink-fraud-detector")
                // Resume from committed offsets; first-ever start reads from the beginning
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new TransactionDeserializer())
                .build();

        WatermarkStrategy<Transaction> watermarks = WatermarkStrategy
                .<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((tx, kafkaTs) -> tx.eventTimeMillis)
                .withIdleness(Duration.ofMinutes(1));

        DataStream<Transaction> transactions = env
                .fromSource(source, watermarks, "kafka-transactions")
                .uid("kafka-transactions-source");

        DataStream<FraudAlert> velocityAlerts = transactions
                .keyBy(tx -> tx.userId)
                .window(SlidingEventTimeWindows.of(Duration.ofSeconds(60), Duration.ofSeconds(10)))
                .aggregate(new CountAggregate(), new VelocityAlertFunction(5))
                .name("velocity-sliding-window")
                .uid("velocity-sliding-window");

        DataStream<FraudAlert> geoAlerts = transactions
                .keyBy(tx -> tx.userId)
                .process(new GeoImpossibilityFunction(900, 50, Duration.ofHours(24)))
                .name("geo-impossibility")
                .uid("geo-impossibility");

        KafkaSink<FraudAlert> sink = KafkaSink.<FraudAlert>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.<FraudAlert>builder()
                        .setTopic("fraud-alerts")
                        .setKeySerializationSchema(a -> a.userId.getBytes())
                        .setValueSerializationSchema(new FraudAlertSerializer())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        velocityAlerts.union(geoAlerts)
                .sinkTo(sink)
                .name("kafka-fraud-alerts")
                .uid("kafka-fraud-alerts-sink");

        env.execute("Fraud Detection - Flink Stream Processor");
    }
}