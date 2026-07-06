package com.checkpoint.iot.stats;

/**
 * Computes mean and population standard deviation over a fixed set of values
 * using the straightforward two-pass definition:
 *
 * <ol>
 *   <li>pass 1 &mdash; sum the values to get the mean;</li>
 *   <li>pass 2 &mdash; sum the squared deviations from that mean to get the variance.</li>
 * </ol>
 *
 * <p>This is the "simple approach" for stage 1: easy to read and to verify against
 * the textbook formula. It buffers all values and scans them twice, which is fine
 * at per-device / per-window scale. (A streaming, single-pass variant is deferred
 * to a later iteration.)
 *
 * <p>Immutable: the values are captured at construction and the stats are derived
 * on demand.
 */
public final class StatisticsCalculator {

    private final double[] values;

    public StatisticsCalculator(double[] values) {
        if (values == null) {
            throw new IllegalArgumentException("values must not be null");
        }
        // Defensive copy so the instance is immutable.
        this.values = values.clone();
    }

    public int count() {
        return values.length;
    }

    /** Arithmetic mean; throws if there are no values. */
    public double mean() {
        if (values.length == 0) {
            throw new IllegalStateException("mean is undefined for zero values");
        }
        double sum = 0.0;
        for (double v : values) {
            sum += v;
        }
        return sum / values.length;
    }

    /**
     * Population standard deviation (divides by N, not N-1). Returns 0 when there
     * is a single value (its variance is 0 by definition).
     */
    public double populationStandardDeviation() {
        if (values.length == 0) {
            throw new IllegalStateException("standard deviation is undefined for zero values");
        }
        double mean = mean();
        double sqDiffSum = 0.0;
        for (double v : values) {
            double diff = v - mean;
            sqDiffSum += diff * diff;
        }
        return Math.sqrt(sqDiffSum / values.length);
    }
}
