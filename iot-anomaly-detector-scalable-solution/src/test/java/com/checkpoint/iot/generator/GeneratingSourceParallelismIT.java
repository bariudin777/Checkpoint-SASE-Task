package com.checkpoint.iot.generator;

import com.checkpoint.iot.model.Reading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Proves {@link GeneratingSourceFunction} actually executes across multiple
 * parallel subtasks &mdash; not just that the pipeline's output is correct (which
 * {@link ScalableAggregatedStdDevJobIT} could pass even at parallelism 1). Each
 * subtask records its own index the moment {@code run()} starts; asserting that
 * every subtask index shows up is direct evidence that generation was actually
 * split across parallel subtasks, not run sequentially by one of them.
 */
class GeneratingSourceParallelismIT {

    @Test
    void generationRunsOnEverySubtaskWhenParallelismIsIncreased() throws Exception {
        int parallelism = 4;
        int deviceCount = 16; // >= parallelism, so every subtask gets at least one device
        SubtaskRecordingSourceFunction.OBSERVED_SUBTASK_INDEXES.clear();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        DataStream<Reading> source = env
                .addSource(new SubtaskRecordingSourceFunction(deviceCount, 5, 60_000, 1, 0.0, 11L))
                .returns(Reading.class);
        source.addSink(new NoOpSink());

        env.execute("generating-source-parallelism-proof");

        assertEquals(parallelism, SubtaskRecordingSourceFunction.OBSERVED_SUBTASK_INDEXES.size(),
                "expected all " + parallelism + " subtasks to have generated at least one device's "
                        + "readings, but only observed subtask indexes: "
                        + SubtaskRecordingSourceFunction.OBSERVED_SUBTASK_INDEXES);
    }

    /** Records its own subtask index before delegating to the real generation logic. */
    private static class SubtaskRecordingSourceFunction extends GeneratingSourceFunction {

        private static final long serialVersionUID = 1L;

        static final Set<Integer> OBSERVED_SUBTASK_INDEXES = ConcurrentHashMap.newKeySet();

        SubtaskRecordingSourceFunction(int deviceCount,
                                       int measurementsPerDevice,
                                       long windowSizeMillis,
                                       int windowCount,
                                       double anomalyProbability,
                                       long seed) {
            super(deviceCount, measurementsPerDevice, windowSizeMillis, windowCount, anomalyProbability, seed);
        }

        @Override
        public void run(SourceContext<Reading> ctx) throws Exception {
            OBSERVED_SUBTASK_INDEXES.add(getRuntimeContext().getIndexOfThisSubtask());
            super.run(ctx);
        }
    }

    private static class NoOpSink implements SinkFunction<Reading> {
        private static final long serialVersionUID = 1L;

        @Override
        public void invoke(Reading value, Context context) {
            // Discard -- this test only cares which subtasks ran, not the output.
        }
    }
}
