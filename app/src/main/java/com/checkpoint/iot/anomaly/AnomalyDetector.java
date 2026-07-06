package com.checkpoint.iot.anomaly;

import com.checkpoint.iot.model.Reading;
import com.checkpoint.iot.stats.StatisticsCalculator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Flags anomalous readings within a single group (in this job: one device's
 * readings for one 1-minute window).
 *
 * <p>An anomaly is a reading whose temperature is more than {@code sigmaThreshold}
 * standard deviations away from the group's mean, i.e. {@code |x - mean| > k*stdev}.
 *
 * <p>Deliberately free of any Flink types so the detection logic is plain OOP and
 * unit-testable on its own; the Flink window function just delegates to it.
 */
public class AnomalyDetector {

    private final double sigmaThreshold;

    public AnomalyDetector(double sigmaThreshold) {
        this.sigmaThreshold = sigmaThreshold;
    }

    /**
     * Returns the readings that are anomalous relative to the whole group.
     *
     * <p>A group of fewer than 2 readings can't yield a meaningful spread, so it is
     * passed through with nothing flagged (documented, not silently wrong).
     */
    public List<Reading> detect(Collection<Reading> readings) {
        List<Reading> anomalies = new ArrayList<>();
        int count = readings.size();
        if (count < 2) {
            return anomalies;
        }

        double[] temps = new double[count];
        int i = 0;
        for (Reading r : readings) {
            temps[i++] = r.getTemperatureC();
        }

        StatisticsCalculator stats = new StatisticsCalculator(temps);
        double mean = stats.mean();
        double stdev = stats.populationStandardDeviation();

        // When every reading is identical, stdev is 0 and nothing can exceed the
        // threshold, so nothing is flagged.
        double limit = sigmaThreshold * stdev;
        for (Reading r : readings) {
            if (Math.abs(r.getTemperatureC() - mean) > limit) {
                anomalies.add(r);
            }
        }
        return anomalies;
    }
}
