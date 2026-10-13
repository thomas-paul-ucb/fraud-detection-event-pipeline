package com.fraud.flink.functions;

import com.fraud.flink.model.Transaction;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Incremental count per window: Flink keeps one number per (user, window)
 * instead of buffering every transaction until the window fires.
 */
public class CountAggregate implements AggregateFunction<Transaction, Long, Long> {
    @Override public Long createAccumulator() { return 0L; }
    @Override public Long add(Transaction tx, Long acc) { return acc + 1; }
    @Override public Long getResult(Long acc) { return acc; }
    @Override public Long merge(Long a, Long b) { return a + b; }
}