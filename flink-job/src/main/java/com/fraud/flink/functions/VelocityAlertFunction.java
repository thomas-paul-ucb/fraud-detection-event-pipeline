package com.fraud.flink.functions;

import com.fraud.flink.model.FraudAlert;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/** Gets a user's transaction count for one window; alerts if it's over the limit. */
public class VelocityAlertFunction extends ProcessWindowFunction<Long, FraudAlert, String, TimeWindow> {
    private final int velocityLimit;

    public VelocityAlertFunction(int velocityLimit) {
        this.velocityLimit = velocityLimit;
    }

    @Override
    public void process(String userId, Context ctx, Iterable<Long> counts, Collector<FraudAlert> out) {
        long count = counts.iterator().next();
        if (count > velocityLimit) {
            TimeWindow w = ctx.window();
            out.collect(new FraudAlert(
                    userId,
                    FraudAlert.VELOCITY,
                    w.getEnd(),
                    count,
                    count + " transactions in " + (w.getEnd() - w.getStart()) / 1000 + "s window"));
        }
    }
}