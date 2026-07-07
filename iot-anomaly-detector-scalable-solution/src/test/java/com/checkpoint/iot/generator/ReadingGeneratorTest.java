package com.checkpoint.iot.generator;

import com.checkpoint.iot.model.Reading;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReadingGeneratorTest {

    @Test
    void generatesDeviceCountTimesMeasurementsPerDeviceTimesWindowCountReadings() {
        List<Reading> readings = new ReadingGenerator(10, 20, 60_000, 3, 0.1, 1L).generate();
        assertEquals(10 * 20 * 3, readings.size());
    }

    @Test
    void generatesTheRequestedNumberOfDistinctDeviceIds() {
        List<Reading> readings = new ReadingGenerator(50, 5, 60_000, 1, 0.0, 2L).generate();
        Set<String> deviceIds = readings.stream().map(Reading::getDeviceId).collect(Collectors.toSet());
        assertEquals(50, deviceIds.size());
    }

    @Test
    void keepsEachWindowsTimestampsWithinThatWindowsBounds() {
        long windowSizeMillis = 60_000;
        int windowCount = 4;
        List<Reading> readings = new ReadingGenerator(3, 10, windowSizeMillis, windowCount, 0.0, 3L).generate();

        for (Reading r : readings) {
            long window = r.getTimestampMillis() / windowSizeMillis;
            assertTrue(window >= 0 && window < windowCount,
                    "timestamp " + r.getTimestampMillis() + " fell outside the expected windows");
        }
    }

    @Test
    void zeroAnomalyProbabilityInjectsNoSpikes() {
        List<Reading> readings = new ReadingGenerator(20, 50, 60_000, 2, 0.0, 4L).generate();
        // A spike is tens of degrees above baseline; ordinary noise never gets close,
        // so this is a safe bound with no risk of a false positive from the test itself.
        readings.forEach(r -> assertTrue(r.getTemperatureC() < 40.0,
                "unexpected high reading with anomalyProbability=0: " + r));
    }

    @Test
    void certainAnomalyProbabilityInjectsExactlyOneSpikePerDevicePerWindow() {
        int deviceCount = 15;
        int windowCount = 2;
        List<Reading> readings =
                new ReadingGenerator(deviceCount, 25, 60_000, windowCount, 1.0, 5L).generate();

        long spikeCount = readings.stream().filter(r -> r.getTemperatureC() > 50.0).count();
        assertEquals((long) deviceCount * windowCount, spikeCount);
    }
}
