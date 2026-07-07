package com.checkpoint.iot.anomaly;

import com.checkpoint.iot.model.Reading;
import com.checkpoint.iot.stats.WelfordAccumulator;

import java.util.ArrayList;
import java.util.List;

/**
 * Flags anomalous readings using an already-finalized {@link WelfordAccumulator}.
 *
 * <p>Unlike the basic solution's {@code AnomalyDetector} (which computes mean, then
 * variance, then flags in three passes over the raw values), the mean/stdev here
 * were already computed incrementally as the readings streamed in, so this only
 * needs one pass over the buffered readings to compare each against the
 * already-known mean/stdev.
 */
public class WelfordAnomalyDetector {

    private final double sigmaThreshold;

    public WelfordAnomalyDetector(double sigmaThreshold) {
        this.sigmaThreshold = sigmaThreshold;
    }

    /**
     * Returns the readings in {@code accumulator} that are anomalous relative to its
     * final mean/stdev.
     *
     * <p>A window of fewer than 2 readings can't yield a meaningful spread, so it is
     * passed through with nothing flagged (documented, not silently wrong).
     */
    public List<Reading> detect(WelfordAccumulator accumulator) {
        List<Reading> anomalies = new ArrayList<>();
        if (accumulator.count() < 2) {
            return anomalies;
        }

        double mean = accumulator.mean();
        // When every reading is identical, stdev is 0 and nothing can exceed the
        // threshold, so nothing is flagged.
        double limit = sigmaThreshold * accumulator.populationStandardDeviation();

        for (Reading r : accumulator.readings()) {
            if (Math.abs(r.getTemperatureC() - mean) > limit) {
                anomalies.add(r);
            }
        }
        return anomalies;
    }
}
