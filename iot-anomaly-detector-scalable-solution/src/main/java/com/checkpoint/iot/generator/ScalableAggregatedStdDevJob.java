package com.checkpoint.iot.generator;

import com.checkpoint.iot.job.AggregatedStdDevJob;
import com.checkpoint.iot.job.ConsoleAnomalySink;
import com.checkpoint.iot.model.Reading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Locale;

/**
 * Stage 3 job: reuses {@link AggregatedStdDevJob}'s Welford-based anomaly-detection
 * pipeline unchanged, but feeds it from {@link GeneratingSourceFunction} instead of
 * parsing a static sample file. Device count, per-device measurement count, window
 * size, and job parallelism are all runtime arguments &mdash; and because the source
 * itself is parallel (each subtask generates its own shard of devices, see
 * {@link GeneratingSourceFunction}), raising parallelism actually spreads both
 * generation and detection across more subtasks instead of just handing a bigger
 * collection to one.
 *
 * <p>Usage: {@code ScalableAggregatedStdDevJob [deviceCount] [measurementsPerDevice]
 * [windowSizeSeconds] [windowCount] [anomalyProbability] [parallelism]} -- every
 * argument is optional and falls back to a default matching the assignment's
 * ~2,000 devices / ~100 measurements-per-minute scale.
 */
public class ScalableAggregatedStdDevJob {

    private static final int DEFAULT_DEVICE_COUNT = 2000;
    private static final int DEFAULT_MEASUREMENTS_PER_DEVICE = 100;
    private static final int DEFAULT_WINDOW_SIZE_SECONDS = 60;
    private static final int DEFAULT_WINDOW_COUNT = 1;
    private static final double DEFAULT_ANOMALY_PROBABILITY = 0.02;
    private static final int DEFAULT_PARALLELISM = Math.max(1, Runtime.getRuntime().availableProcessors());

    // Fixed so a given set of arguments always produces the same data, run to run.
    private static final long GENERATOR_SEED = 42L;

    public static void main(String[] args) throws Exception {
        int deviceCount = intArg(args, 0, DEFAULT_DEVICE_COUNT);
        int measurementsPerDevice = intArg(args, 1, DEFAULT_MEASUREMENTS_PER_DEVICE);
        int windowSizeSeconds = intArg(args, 2, DEFAULT_WINDOW_SIZE_SECONDS);
        int windowCount = intArg(args, 3, DEFAULT_WINDOW_COUNT);
        double anomalyProbability = doubleArg(args, 4, DEFAULT_ANOMALY_PROBABILITY);
        int parallelism = intArg(args, 5, DEFAULT_PARALLELISM);

        System.out.printf(Locale.US,
                "Generating %d devices x %d measurements/window x %d window(s), "
                        + "window size %ds, anomaly probability %.3f, parallelism %d%n",
                deviceCount, measurementsPerDevice, windowCount, windowSizeSeconds,
                anomalyProbability, parallelism);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        DataStream<Reading> source = env
                .addSource(new GeneratingSourceFunction(deviceCount, measurementsPerDevice,
                        windowSizeSeconds * 1000L, windowCount, anomalyProbability, GENERATOR_SEED))
                .returns(Reading.class)
                .name("generated-readings");

        DataStream<String> anomalies = AggregatedStdDevJob.buildAnomalyStream(
                source, Time.seconds(windowSizeSeconds));
        anomalies.addSink(new ConsoleAnomalySink());

        env.execute("IoT Temperature Anomaly Detection - Scalable Generated (Stage 3)");
    }

    private static int intArg(String[] args, int index, int defaultValue) {
        return args.length > index ? Integer.parseInt(args[index]) : defaultValue;
    }

    private static double doubleArg(String[] args, int index, double defaultValue) {
        return args.length > index ? Double.parseDouble(args[index]) : defaultValue;
    }
}
