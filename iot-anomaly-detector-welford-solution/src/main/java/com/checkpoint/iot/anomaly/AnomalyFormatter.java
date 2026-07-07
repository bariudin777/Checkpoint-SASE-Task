package com.checkpoint.iot.anomaly;

import com.checkpoint.iot.model.Reading;

import java.util.Locale;

/**
 * Formats an anomalous reading into the exact console line the assignment asks for:
 *
 * <pre>Device 455weg75uew, measurement 95.4 C, time 45587456</pre>
 */
public final class AnomalyFormatter {

    private AnomalyFormatter() {
    }

    public static String format(Reading reading) {
        // Locale.US so the decimal separator is always '.', regardless of the host.
        return String.format(Locale.US,
                "Device %s, measurement %.1f C, time %d",
                reading.getDeviceId(),
                reading.getTemperatureC(),
                reading.getTimestampMillis());
    }
}
