package com.checkpoint.iot.job;

import com.checkpoint.iot.model.Reading;
import com.checkpoint.iot.stats.WelfordAccumulator;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Folds each reading into a running {@link WelfordAccumulator} as it arrives.
 *
 * <p>This is the streaming-native alternative to the basic solution's
 * {@code ProcessWindowFunction}, which buffers the whole window and only computes
 * mean/variance once it fires. Here the mean/variance are updated incrementally on
 * every {@link #add}, so they're already final by the time the window closes —
 * {@link WelfordAnomalyWindowFunction} just has to run the flagging pass.
 */
public class WelfordAggregateFunction
        implements AggregateFunction<Reading, WelfordAccumulator, WelfordAccumulator> {

    private static final long serialVersionUID = 1L;

    @Override
    public WelfordAccumulator createAccumulator() {
        return new WelfordAccumulator();
    }

    @Override
    public WelfordAccumulator add(Reading reading, WelfordAccumulator accumulator) {
        accumulator.add(reading);
        return accumulator;
    }

    @Override
    public WelfordAccumulator getResult(WelfordAccumulator accumulator) {
        return accumulator;
    }

    @Override
    public WelfordAccumulator merge(WelfordAccumulator a, WelfordAccumulator b) {
        return a.merge(b);
    }
}
