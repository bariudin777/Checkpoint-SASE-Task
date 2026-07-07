package com.checkpoint.iot.job;

import com.checkpoint.iot.anomaly.AnomalyFormatter;
import com.checkpoint.iot.anomaly.WelfordAnomalyDetector;
import com.checkpoint.iot.model.Reading;
import com.checkpoint.iot.stats.WelfordAccumulator;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Runs once per window, after {@link WelfordAggregateFunction} has already folded
 * every reading into a single finalized {@link WelfordAccumulator}. Flink invokes a
 * paired {@code ProcessWindowFunction} with a one-element {@code Iterable} when used
 * via {@code window.aggregate(aggregateFunction, processWindowFunction)}.
 */
public class WelfordAnomalyWindowFunction
        extends ProcessWindowFunction<WelfordAccumulator, String, String, TimeWindow> {

    private static final long serialVersionUID = 1L;

    private final double sigmaThreshold;
    private transient WelfordAnomalyDetector detector;

    public WelfordAnomalyWindowFunction(double sigmaThreshold) {
        this.sigmaThreshold = sigmaThreshold;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        this.detector = new WelfordAnomalyDetector(sigmaThreshold);
    }

    @Override
    public void process(String deviceId,
                        Context context,
                        Iterable<WelfordAccumulator> elements,
                        Collector<String> out) {
        WelfordAccumulator accumulator = elements.iterator().next();
        for (Reading anomaly : detector.detect(accumulator)) {
            out.collect(AnomalyFormatter.format(anomaly));
        }
    }
}
