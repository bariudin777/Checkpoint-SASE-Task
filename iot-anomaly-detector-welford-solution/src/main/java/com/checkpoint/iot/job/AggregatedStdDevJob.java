package com.checkpoint.iot.job;

import com.checkpoint.iot.io.ReadingFileParser;
import com.checkpoint.iot.model.Reading;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;

/**
 * Stage 2 job (Implementation B &mdash; incremental / Welford).
 *
 * <p>Same pipeline shape and output format as the basic solution: read sampled
 * readings from a file &rarr; per device, tumble a 1-minute event-time window &rarr;
 * flag readings more than 3&sigma; from the window mean &rarr; print anomalies to the
 * console. The difference from the basic solution is entirely in *how* the
 * mean/stdev are computed &mdash; see
 * {@link WelfordAggregateFunction} and {@link WelfordAnomalyWindowFunction}.
 *
 * <p>Still file-based, no Kafka: this stage is about comparing the two statistics
 * implementations against the same input, not about scaling the input pipeline.
 */
public class AggregatedStdDevJob {

    /** An anomaly is a reading this many standard deviations from the mean. */
    public static final double SIGMA_THRESHOLD = 3.0;

    /** Averaging window length required by the assignment. */
    public static final Time WINDOW_SIZE = Time.minutes(1);

    public static void main(String[] args) throws Exception {
        List<Reading> readings = loadReadings(args);
        System.out.println("Loaded " + readings.size() + " readings; detecting anomalies...");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Reading> source = env.fromCollection(readings);
        DataStream<String> anomalies = buildAnomalyStream(source);
        anomalies.addSink(new ConsoleAnomalySink());

        env.execute("IoT Temperature Anomaly Detection - Aggregated StdDev (Stage 2)");
    }

    /**
     * Applies the anomaly-detection pipeline to a stream of readings, using the
     * default 1-minute window. Kept separate from {@link #main} and free of any
     * specific source/sink so the full-flow test can feed it a fixed collection and
     * capture its output.
     */
    public static DataStream<String> buildAnomalyStream(DataStream<Reading> source) {
        return buildAnomalyStream(source, WINDOW_SIZE);
    }

    /**
     * Same pipeline as {@link #buildAnomalyStream(DataStream)}, but with the window
     * length as a parameter instead of the hardcoded 1-minute default &mdash; lets a
     * caller (e.g. the scalable/generated-data job) drive the window size at runtime.
     */
    public static DataStream<String> buildAnomalyStream(DataStream<Reading> source, Time windowSize) {
        WatermarkStrategy<Reading> watermarks = WatermarkStrategy
                .<Reading>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((reading, ts) -> reading.getTimestampMillis());

        return source
                .assignTimestampsAndWatermarks(watermarks)
                .keyBy(Reading::getDeviceId)
                .window(TumblingEventTimeWindows.of(windowSize))
                .aggregate(new WelfordAggregateFunction(), new WelfordAnomalyWindowFunction(SIGMA_THRESHOLD));
    }

    /**
     * Loads readings from the JSON-lines file path given as {@code args[0]}.
     */
    private static List<Reading> loadReadings(String[] args) {
        if (args.length == 0) {
            throw new IllegalArgumentException(
                    "Usage: AggregatedStdDevJob <path-to-readings.jsonl>");
        }
        return new ReadingFileParser().parse(Paths.get(args[0]));
    }
}
