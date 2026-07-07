package com.checkpoint.iot.job;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * Prints each already-formatted anomaly line to stdout verbatim.
 *
 * <p>Used instead of {@code DataStream.print()} so the output matches the
 * assignment's example line exactly, without Flink's {@code "<subtaskId>> "} prefix.
 */
public class ConsoleAnomalySink implements SinkFunction<String> {

    private static final long serialVersionUID = 1L;

    @Override
    public void invoke(String value, Context context) {
        System.out.println(value);
    }
}
