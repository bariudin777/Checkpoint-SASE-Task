CheckPoint SASE - Task


IoT Temperature Anomaly Detection

Write a Flink job that flags anomalous temperature readings from IoT devices.

• ~2,000 devices, ~100 measurements per device per minute.
• An anomaly = a reading 3 standard deviations from the 1-minute average.
• Print anomalies to the console, e.g.:
    Device 455weg75uew, measurement 95.4 C, time 45587456

Use Java and Flink. Everything else is your call - build tool, layout, and how you generate the input (a small generator is fine).
Use AI if that's how you normally work; We'll just ask you to explain your choices.

## Modules

Three Gradle modules, each runnable independently, building up from "does the math work" to "does it scale."

### 1. `iot-anomaly-detector-basic-solution` - naive two-pass

The textbook implementation: per device, tumble a 1-minute event-time window, buffer every reading in Flink state (`ProcessWindowFunction`), then at fire time loop over the buffer three times - once for the mean, once for the variance, once to flag readings more than 3 sigma away. Reads a small static sample file (`src/test/resources/sample-data/readings.jsonl`, two devices, one injected spike).

Easy to read and to verify against the textbook formula, at the cost of buffering every raw reading in state until the window fires.

**Run it:**
```
gradle :iot-anomaly-detector-basic-solution:run
```
**Test it:**
```
gradle :iot-anomaly-detector-basic-solution:test
```

### 2. `iot-anomaly-detector-welford-solution` - incremental (Welford)

Same pipeline shape, same output, same sample file - but the mean/variance are updated incrementally as each reading arrives (`AggregateFunction` running Welford's online algorithm) instead of being recomputed from a buffered list. Mean/stdev are already final the instant the window's last reading arrives, so only one pass (the flagging comparison) is needed at fire time, not three. The individual-reading flagging step still needs the raw readings once the stats are final, so it isn't fully O(1) state - see the code comments in `WelfordAccumulator` for why.

**Run it:**
```
gradle :iot-anomaly-detector-welford-solution:run
```
**Test it:**
```
gradle :iot-anomaly-detector-welford-solution:test
```

### 3. `iot-anomaly-detector-scalable-solution` - dynamic scale, generated data

Reuses the Welford pipeline from module 2 unchanged (depends on it as a Gradle project dependency - no duplicated code) but swaps the static file for a `GeneratingSourceFunction`: a parallel Flink source where each subtask generates its own shard of devices on the fly, instead of loading one static file or one big in-memory collection into a single subtask. Three things that were hardcoded elsewhere are runtime arguments here:

- device count
- measurements per device (per window)
- window size

...plus window count and anomaly-injection probability (how many devices get a temperature spike per window), and job parallelism. Raising parallelism actually spreads generation and detection across more subtasks - proven directly by `GeneratingSourceParallelismIT`, which records which subtask indices ran and asserts all of them fired.

**Run it** (defaults to the assignment's actual scale: 2,000 devices, 100 measurements/device/window, 60s window, parallelism = available CPU cores):
```
gradle :iot-anomaly-detector-scalable-solution:run
```
**Run it with custom scale** - args are `[deviceCount] [measurementsPerDevice] [windowSizeSeconds] [windowCount] [anomalyProbability] [parallelism]`, all optional:
```
gradle :iot-anomaly-detector-scalable-solution:run --args="20 15 10 1 0.3 4"
```
**Test it:**
```
gradle :iot-anomaly-detector-scalable-solution:test
```

**What to expect at default scale:** with 2,000 devices x 100 readings, the console prints on the order of ~500 anomaly lines, not just the ~40 devices whose spike was actually injected (`anomalyProbability = 0.02` -> ~40 devices). The other ~450+ are real per-spec anomalies too, not a bug: with `n = 100` samples in a window, "3 standard deviations from that same window's own mean" has roughly a 0.27% two-tailed chance of firing on pure noise per sample, so `~100 x 0.27% x 2,000 devices ~= 500` noise-only flags are statistically expected. It's an inherent property of self-referential 3-sigma thresholds at this sample size, not something the pipeline gets wrong - worth knowing when reading the output.

### Build everything / run all tests

```
gradle build
```

## Sample run output

The [`logs/`](logs/) directory has captured console output from actual runs of all three modules, so the results above are reproducible evidence rather than just claims:

- [`logs/basic-solution-run.log`](logs/basic-solution-run.log)
- [`logs/welford-solution-run.log`](logs/welford-solution-run.log)
- [`logs/scalable-solution-run.log`](logs/scalable-solution-run.log) - default assignment-scale run (2,000 devices)
- [`logs/scalable-solution-custom-args-run.log`](logs/scalable-solution-custom-args-run.log) - small custom-scale run (`20 15 10 1 0.3 4`)