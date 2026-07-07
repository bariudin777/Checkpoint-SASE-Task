package com.checkpoint.iot.generator;

import com.checkpoint.iot.job.AggregatedStdDevJob;
import com.checkpoint.iot.model.Reading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Full-flow test: {@link GeneratingSourceFunction} feeding
 * {@link AggregatedStdDevJob}'s real windowed pipeline (the same one used by the
 * file-based Welford job) &mdash; run with parallelism &gt; 1, the same way
 * {@link ScalableAggregatedStdDevJob#main} does, so these prove the pipeline stays
 * correct under real parallel execution, not just against a bigger single-subtask
 * collection. See {@link GeneratingSourceParallelismIT} for proof that multiple
 * subtasks are actually used.
 */
class ScalableAggregatedStdDevJobIT {

    @BeforeEach
    void clear() {
        CollectSink.VALUES.clear();
    }

    @Test
    void flagsExactlyOneSpikePerDeviceWhenAnomalyProbabilityIsCertain() throws Exception {
        int deviceCount = 20;
        int measurementsPerDevice = 30;
        long windowSizeMillis = 10_000;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStream<Reading> source = env
                .addSource(new GeneratingSourceFunction(
                        deviceCount, measurementsPerDevice, windowSizeMillis, 1, 1.0, 42L))
                .returns(Reading.class);
        AggregatedStdDevJob.buildAnomalyStream(source, Time.milliseconds(windowSizeMillis))
                .addSink(new CollectSink());

        env.execute("scalable-generated-small-test");

        assertEquals(deviceCount, CollectSink.VALUES.size(),
                "expected exactly one flagged spike per device, got: " + CollectSink.VALUES);
    }

    @Test
    void scalesToHundredsOfDevicesWithDynamicWindowSize() throws Exception {
        int deviceCount = 300;
        int measurementsPerDevice = 50;
        long windowSizeMillis = 30_000; // dynamic: not the default 60s

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStream<Reading> source = env
                .addSource(new GeneratingSourceFunction(
                        deviceCount, measurementsPerDevice, windowSizeMillis, 1, 0.05, 7L))
                .returns(Reading.class);
        AggregatedStdDevJob.buildAnomalyStream(source, Time.milliseconds(windowSizeMillis))
                .addSink(new CollectSink());

        env.execute("scalable-generated-scale-test");

        // Probability-driven, so not asserting an exact count -- just that the
        // pipeline ran end-to-end at this scale and flagged a small minority.
        assertTrue(CollectSink.VALUES.size() > 0 && CollectSink.VALUES.size() < deviceCount,
                "expected some, but far fewer than " + deviceCount + " anomalies, got: "
                        + CollectSink.VALUES.size());
    }
}
