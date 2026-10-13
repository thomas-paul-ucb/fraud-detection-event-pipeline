package com.fraud.flink.functions;

import com.fraud.flink.model.FraudAlert;
import com.fraud.flink.model.Transaction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Geographic impossibility: stores each user's last location in Flink keyed state
 * and flags a transaction when the implied travel speed since the previous one
 * is physically implausible.
 */
public class GeoImpossibilityFunction extends KeyedProcessFunction<String, Transaction, FraudAlert> {

    /** Last location seen for a user. */
    public static class LastLocation {
        public double lat;
        public double lon;
        public long eventTimeMillis;

        public LastLocation() {}

        LastLocation(double lat, double lon, long eventTimeMillis) {
            this.lat = lat;
            this.lon = lon;
            this.eventTimeMillis = eventTimeMillis;
        }
    }

    private static final double EARTH_RADIUS_KM = 6371.0;

    private final double maxSpeedKmh;
    private final double minDistanceKm;
    private final Duration stateTtl;

    private transient ValueState<LastLocation> lastLocation;

    public GeoImpossibilityFunction(double maxSpeedKmh, double minDistanceKm, Duration stateTtl) {
        this.maxSpeedKmh = maxSpeedKmh;
        this.minDistanceKm = minDistanceKm;
        this.stateTtl = stateTtl;
    }

    @Override
    public void open(OpenContext ctx) {
        ValueStateDescriptor<LastLocation> desc =
                new ValueStateDescriptor<>("last-location", TypeInformation.of(LastLocation.class));
        // Forget users who've been inactive for a while so state doesn't grow forever
        desc.enableTimeToLive(StateTtlConfig.newBuilder(stateTtl)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build());
        lastLocation = getRuntimeContext().getState(desc);
    }

    @Override
    public void processElement(Transaction tx, Context ctx, Collector<FraudAlert> out) throws Exception {
        if (!tx.hasLocation()) return;

        LastLocation prev = lastLocation.value();
        if (prev != null) {
            double distanceKm = haversineKm(prev.lat, prev.lon, tx.lat, tx.lon);
            long dtMillis = Math.abs(tx.eventTimeMillis - prev.eventTimeMillis);
            // Floor at 1s so near-simultaneous events give a huge (not infinite) speed
            double hours = Math.max(dtMillis, 1000L) / 3_600_000.0;
            double speedKmh = distanceKm / hours;

            if (distanceKm >= minDistanceKm && speedKmh > maxSpeedKmh) {
                out.collect(new FraudAlert(
                        tx.userId,
                        FraudAlert.GEO_IMPOSSIBILITY,
                        tx.eventTimeMillis,
                        Math.round(speedKmh),
                        String.format("moved %.0f km in %.1f s", distanceKm, dtMillis / 1000.0)));
            }
            // A late (out-of-order) event shouldn't rewind the user's position
            if (tx.eventTimeMillis < prev.eventTimeMillis) return;
        }
        lastLocation.update(new LastLocation(tx.lat, tx.lon, tx.eventTimeMillis));
    }

    /** Great-circle distance between two lat/lon points. */
    static double haversineKm(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        return 2 * EARTH_RADIUS_KM * Math.asin(Math.sqrt(a));
    }
}