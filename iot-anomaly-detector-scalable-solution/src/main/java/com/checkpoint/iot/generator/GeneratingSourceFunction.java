package com.checkpoint.iot.generator;

import com.checkpoint.iot.model.Reading;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.List;

/**
 * Parallel Flink source that generates readings directly inside each parallel
 * subtask, instead of materializing the whole dataset in the client and shipping
 * it as one collection ({@code env.fromCollection} does this, which pins the
 * source to a single subtask no matter how much parallelism the rest of the job
 * has).
 *
 * <p>Devices are sharded round-robin across subtasks
 * ({@code deviceIndex % numSubtasks == subtaskIndex}), so raising the job's
 * parallelism raises how much of the generation work actually runs concurrently.
 * Because {@link ReadingGenerator#generateForDevice} seeds each device
 * independently, the data produced is identical regardless of how many subtasks
 * are used or which one generates a given device.
 */
public class GeneratingSourceFunction extends RichParallelSourceFunction<Reading> {

    private static final long serialVersionUID = 1L;

    private final int deviceCount;
    private final int measurementsPerDevice;
    private final long windowSizeMillis;
    private final int windowCount;
    private final double anomalyProbability;
    private final long seed;

    private volatile boolean running = true;

    public GeneratingSourceFunction(int deviceCount,
                                     int measurementsPerDevice,
                                     long windowSizeMillis,
                                     int windowCount,
                                     double anomalyProbability,
                                     long seed) {
        this.deviceCount = deviceCount;
        this.measurementsPerDevice = measurementsPerDevice;
        this.windowSizeMillis = windowSizeMillis;
        this.windowCount = windowCount;
        this.anomalyProbability = anomalyProbability;
        this.seed = seed;
    }

    @Override
    public void run(SourceContext<Reading> ctx) throws Exception {
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        int numSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();

        ReadingGenerator generator = new ReadingGenerator(
                deviceCount, measurementsPerDevice, windowSizeMillis, windowCount, anomalyProbability, seed);

        for (int device = subtaskIndex; running && device < deviceCount; device += numSubtasks) {
            List<Reading> deviceReadings = generator.generateForDevice(device);
            synchronized (ctx.getCheckpointLock()) {
                for (Reading reading : deviceReadings) {
                    ctx.collect(reading);
                }
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
