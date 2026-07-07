package com.checkpoint.iot.stats;

import com.checkpoint.iot.model.Reading;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Running mean/variance for a device's window, updated one reading at a time via
 * Welford's online algorithm instead of buffering every value and scanning it
 * twice: {@code mean}/{@code m2} are correct the instant the last reading of the
 * window is added, no separate averaging pass required.
 *
 * <p>Still keeps the raw readings alongside the running stats: reporting *which*
 * individual reading was anomalous can only be decided once the window's final
 * mean/stdev are known, so the raw values are kept for that one comparison pass at
 * fire time (see {@link com.checkpoint.iot.job.WelfordAggregateFunction} for why
 * this is one pass instead of the naive approach's three).
 */
public class WelfordAccumulator implements Serializable {

    private static final long serialVersionUID = 1L;

    private long count = 0;
    private double mean = 0;
    private double m2 = 0; // sum of squared differences from the running mean
    private final List<Reading> readings = new ArrayList<>();

    /** Folds one more reading into the running mean/variance in O(1). */
    public void add(Reading reading) {
        readings.add(reading);
        count++;
        double delta = reading.getTemperatureC() - mean;
        mean += delta / count;
        double delta2 = reading.getTemperatureC() - mean;
        m2 += delta * delta2;
    }

    /** Combines two partial accumulators (Chan et al. parallel variance formula). */
    public WelfordAccumulator merge(WelfordAccumulator other) {
        WelfordAccumulator merged = new WelfordAccumulator();
        merged.readings.addAll(this.readings);
        merged.readings.addAll(other.readings);

        long n = this.count + other.count;
        if (n == 0) {
            return merged;
        }
        double delta = other.mean - this.mean;
        merged.count = n;
        merged.mean = this.mean + delta * other.count / n;
        merged.m2 = this.m2 + other.m2 + delta * delta * this.count * other.count / n;
        return merged;
    }

    public long count() {
        return count;
    }

    /** Arithmetic mean; throws if no readings have been added yet. */
    public double mean() {
        if (count == 0) {
            throw new IllegalStateException("mean is undefined for zero values");
        }
        return mean;
    }

    /** Population variance (divides by N, not N-1). */
    public double populationVariance() {
        if (count == 0) {
            throw new IllegalStateException("variance is undefined for zero values");
        }
        return m2 / count;
    }

    public double populationStandardDeviation() {
        return Math.sqrt(populationVariance());
    }

    /** The raw readings folded into this accumulator so far, in arrival order. */
    public List<Reading> readings() {
        return readings;
    }
}
