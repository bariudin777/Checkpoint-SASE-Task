package com.checkpoint.iot.generator;

import com.checkpoint.iot.model.Reading;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Generates synthetic {@link Reading}s in memory instead of reading them from a
 * file, so the pipeline can be exercised at whatever scale is asked for: device
 * count, per-device measurement count, and window size are all runtime
 * parameters instead of a fixed sample file.
 *
 * <p>Each device gets a fixed baseline temperature (a per-device offset plus small
 * noise per reading) and, with {@code anomalyProbability} chance per window, one of
 * its readings in that window is replaced with a large spike, so anomalies show up
 * without every run being identical.
 *
 * <p>Each device's data is generated from its own seeded {@link Random}, derived
 * from the shared seed and the device index rather than drawn from one shared,
 * sequentially-advanced generator. That makes {@link #generateForDevice} safe to
 * call for any device, in any order, from any thread &mdash; which is what lets
 * {@link GeneratingSourceFunction} generate different devices concurrently across
 * parallel subtasks and still get the same data a sequential run would produce.
 */
public class ReadingGenerator {

    private static final double BASELINE_CENTER_C = 20.0;
    private static final double BASELINE_SPREAD_C = 5.0;   // per-device baseline varies +/- this much
    private static final double NOISE_STDDEV_C = 0.3;      // normal reading-to-reading noise
    private static final double SPIKE_OFFSET_C = 60.0;     // injected anomaly offset from baseline

    // Fibonacci hashing multiplier: spreads sequential device indices across the
    // seed space so nearby indices don't produce correlated Random streams.
    private static final long DEVICE_SEED_MULTIPLIER = 0x9E3779B97F4A7C15L;

    private final int deviceCount;
    private final int measurementsPerDevice;
    private final long windowSizeMillis;
    private final int windowCount;
    private final double anomalyProbability;
    private final long seed;

    public ReadingGenerator(int deviceCount,
                             int measurementsPerDevice,
                             long windowSizeMillis,
                             int windowCount,
                             double anomalyProbability,
                             long seed) {
        if (deviceCount < 1 || measurementsPerDevice < 1 || windowSizeMillis < 1 || windowCount < 1) {
            throw new IllegalArgumentException(
                    "deviceCount, measurementsPerDevice, windowSizeMillis and windowCount must all be >= 1");
        }
        this.deviceCount = deviceCount;
        this.measurementsPerDevice = measurementsPerDevice;
        this.windowSizeMillis = windowSizeMillis;
        this.windowCount = windowCount;
        this.anomalyProbability = anomalyProbability;
        this.seed = seed;
    }

    public int deviceCount() {
        return deviceCount;
    }

    /** Generates every device's readings for every window, in device-index order. */
    public List<Reading> generate() {
        List<Reading> readings = new ArrayList<>(deviceCount * measurementsPerDevice * windowCount);
        for (int device = 0; device < deviceCount; device++) {
            readings.addAll(generateForDevice(device));
        }
        return readings;
    }

    /**
     * Generates one device's readings across all windows. Independent of every
     * other device: safe to call for any device index, in any order, concurrently.
     */
    public List<Reading> generateForDevice(int deviceIndex) {
        Random random = new Random(seed ^ (deviceIndex * DEVICE_SEED_MULTIPLIER));
        String deviceId = String.format("device-%04d", deviceIndex);
        double baseline = BASELINE_CENTER_C + (random.nextDouble() * 2 - 1) * BASELINE_SPREAD_C;
        long measurementIntervalMillis = Math.max(1, windowSizeMillis / measurementsPerDevice);

        List<Reading> readings = new ArrayList<>(measurementsPerDevice * windowCount);
        for (int window = 0; window < windowCount; window++) {
            long windowStartMillis = (long) window * windowSizeMillis;
            boolean injectSpike = random.nextDouble() < anomalyProbability;
            int spikeIndex = injectSpike ? random.nextInt(measurementsPerDevice) : -1;

            for (int i = 0; i < measurementsPerDevice; i++) {
                long timestampMillis = windowStartMillis + i * measurementIntervalMillis;
                double temperature = (i == spikeIndex)
                        ? baseline + SPIKE_OFFSET_C
                        : baseline + random.nextGaussian() * NOISE_STDDEV_C;
                readings.add(new Reading(deviceId, temperature, timestampMillis));
            }
        }
        return readings;
    }
}
