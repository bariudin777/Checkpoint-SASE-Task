package com.checkpoint.iot.job;

import com.checkpoint.iot.io.ReadingFileParser;
import com.checkpoint.iot.model.Reading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Full-flow test: same sampled file and same expected output as the basic
 * solution's {@code SimpleStdDevJobIT} &mdash; this confirms the incremental
 * (Welford) implementation agrees with the naive two-pass one on identical input.
 */
class AggregatedStdDevJobIT {

    @BeforeEach
    void clear() {
        CollectSink.VALUES.clear();
    }

    @Test
    void detectsExactlyTheInjectedSpikeAcrossBothDevices() throws Exception {
        List<Reading> readings = loadSampleReadings();
        assertEquals(31, readings.size(), "sanity check on the sample file");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Single subtask keeps the shared collector deterministic in this test.
        env.setParallelism(1);

        DataStream<Reading> source = env.fromCollection(readings);
        AggregatedStdDevJob.buildAnomalyStream(source).addSink(new CollectSink());

        env.execute("full-flow-test");

        System.out.println("Detected anomalies: " + CollectSink.VALUES);

        // sensor-A's 95.4 spike is the only anomaly; sensor-B is all-normal.
        assertEquals(1, CollectSink.VALUES.size(),
                "expected exactly one anomaly, got: " + CollectSink.VALUES);
        assertEquals("Device sensor-A, measurement 95.4 C, time 67500", CollectSink.VALUES.get(0));
        assertTrue(CollectSink.VALUES.stream().noneMatch(line -> line.contains("sensor-B")),
                "no sensor-B reading should be flagged");
    }

    private static List<Reading> loadSampleReadings() throws Exception {
        try (InputStream in =
                     AggregatedStdDevJob.class.getResourceAsStream("/sample-data/readings.jsonl")) {
            assertNotNull(in, "sample data resource must be on the test classpath");
            return Collections.unmodifiableList(new ReadingFileParser().parse(in));
        }
    }
}
