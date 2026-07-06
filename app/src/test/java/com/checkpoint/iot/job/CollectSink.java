package com.checkpoint.iot.job;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Test-only sink that captures the pipeline's output lines in a static list, in
 * place of the console sink used in production. Static because Flink serializes
 * sink instances to the (local) task; the values land back in this JVM.
 */
class CollectSink implements SinkFunction<String> {

    static final List<String> VALUES = new CopyOnWriteArrayList<>();

    @Override
    public void invoke(String value, Context context) {
        VALUES.add(value);
    }
}
