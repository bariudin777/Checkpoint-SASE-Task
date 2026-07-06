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
 * Stage 1 job (Implementation A &mdash; simple / naive two-pass).
 *
 * <p>Flow: read sampled readings from a file &rarr; per device, tumble a 1-minute
 * event-time window &rarr; flag readings more than 3&sigma; from the window mean
 * &rarr; print anomalies to the console.
 *
 * <p>No Kafka, no cluster: it runs entirely in a local Flink environment against a
 * static file, which is all stage 1 needs. Later iterations swap the file source
 * for a Kafka source and add the streaming (Welford) implementation.
 */
public class SimpleStdDevJob {

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

        env.execute("IoT Temperature Anomaly Detection - Simple StdDev (Stage 1)");
    }

    /**
     * Applies the anomaly-detection pipeline to a stream of readings. Kept separate
     * from {@link #main} and free of any specific source/sink so the full-flow test
     * can feed it a fixed collection and capture its output.
     */
    public static DataStream<String> buildAnomalyStream(DataStream<Reading> source) {
        WatermarkStrategy<Reading> watermarks = WatermarkStrategy
                .<Reading>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((reading, ts) -> reading.getTimestampMillis());

        return source
                .assignTimestampsAndWatermarks(watermarks)
                .keyBy(Reading::getDeviceId)
                .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
                .process(new AnomalyWindowFunction(SIGMA_THRESHOLD));
    }

    /**
     * Loads readings from the JSON-lines file path given as {@code args[0]}.
     *
     * <p>The input is a real file, not a bundled resource: the sample data lives in
     * the test package, and {@code gradle run} points here at that sample file.
     */
    private static List<Reading> loadReadings(String[] args) {
        if (args.length == 0) {
            throw new IllegalArgumentException(
                    "Usage: SimpleStdDevJob <path-to-readings.jsonl>");
        }
        return new ReadingFileParser().parse(Paths.get(args[0]));
    }
}
