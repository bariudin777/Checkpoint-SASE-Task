package com.checkpoint.iot.job;

import com.checkpoint.iot.anomaly.AnomalyDetector;
import com.checkpoint.iot.anomaly.AnomalyFormatter;
import com.checkpoint.iot.model.Reading;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Flink window function for the simple (naive two-pass) approach.
 *
 * <p>For each device's 1-minute window it buffers every reading, hands them to the
 * plain-OOP {@link AnomalyDetector}, and emits one formatted console line per
 * anomalous reading. The statistics/anomaly logic itself lives outside Flink so it
 * can be unit-tested without a cluster.
 */
public class AnomalyWindowFunction
        extends ProcessWindowFunction<Reading, String, String, TimeWindow> {

    private static final long serialVersionUID = 1L;

    private final double sigmaThreshold;
    private transient AnomalyDetector detector;

    public AnomalyWindowFunction(double sigmaThreshold) {
        this.sigmaThreshold = sigmaThreshold;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        this.detector = new AnomalyDetector(sigmaThreshold);
    }

    @Override
    public void process(String deviceId,
                        Context context,
                        Iterable<Reading> elements,
                        Collector<String> out) {
        // The ProcessWindowFunction hands us the full buffered window; materialize
        // it so the detector can do its two passes (mean, then deviations).
        List<Reading> readings = new ArrayList<>();
        for (Reading r : elements) {
            readings.add(r);
        }

        for (Reading anomaly : detector.detect(readings)) {
            out.collect(AnomalyFormatter.format(anomaly));
        }
    }
}
