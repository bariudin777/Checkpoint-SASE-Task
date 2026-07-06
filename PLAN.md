# IoT Temperature Anomaly Detection — Implementation Plan

## 0. Assumptions

- "1-minute average" and "3 standard deviations" are computed **per device**, not
  globally — each sensor has its own baseline, so the mean/stdev must be scoped to
  that device's own readings in the window. `keyBy(deviceId)` gives us this for free.
- Anomaly rule: for a reading `x` in a device's 1-minute tumbling window with mean
  `μ` and population stdev `σ`: flag if `|x - μ| > 3σ`.
- A window needs at least 2 samples to compute a meaningful stdev; windows with
  fewer samples are passed through with no anomalies flagged (documented, not
  silently wrong).
- Scale target: ~2,000 devices × 100 msg/min ≈ 3,300 events/sec sustained. Not
  "big data" scale, but the design should have an obvious path to scale further
  without a rewrite.
- Keep the stack minimal: Kafka is the only extra moving part, added specifically
  because the assignment calls it out as a scaling lever. No schema registry, no
  external DB, no orchestration layer — those would be over-engineering for this
  scope.

## 1. Architecture Overview

```
[Generator A: file/JSON replay]  --\
                                     >-- Kafka topic "iot-readings" --> [Flink Job] --> console (anomalies)
[Generator B: continuous, scaled] --/                                       |
                                                                              --> raw sink (optional, see §7)
```

- Both generators write the same JSON schema to the same Kafka topic, so the Flink
  job doesn't care which one produced the data.
- Kafka decouples generation rate from processing rate and is the natural scale-out
  point (partition by `deviceId`, add partitions/consumers to scale).
- The Flink job is the only "smart" component; console sink keeps the output
  trivially inspectable per the assignment's example line.

## 2. Data Model

```java
public class Reading {
    public String deviceId;
    public double temperatureC;
    public long timestampMillis; // event time, producer-assigned
}
```

Plain POJO (public fields, no-arg constructor) so Flink's type extraction and
serialization work without extra config.

JSON on the wire:
```json
{"deviceId": "455weg75uew", "temperatureC": 95.4, "timestampMillis": 45587456}
```

## 3. Data Generation — two generators

### Generator A — simple / file-based (`generator-simple`)
- Reads a static JSON file (or JSON-lines file) of pre-canned readings, one record
  at a time, and either:
  - writes them straight to Kafka with a small sleep between records, or
  - is used directly as a Flink `FileSource` in tests, bypassing Kafka entirely.
- Purpose: deterministic input for local dev, demos, and integration tests. No
  timing pressure, no concurrency — just correctness.
- Lives in `src/test/resources/sample-data/*.json` for test use, and a standalone
  `main()` for manual demo use against Kafka.

### Generator B — continuous / scalable (`generator-continuous`)
- Standalone Java process (own `main()`, own Docker container), independent of the
  Flink job.
- Simulates 2,000 virtual devices, each emitting ~100 readings/min (~1 every
  600ms/device), using a thread pool or a single event-loop with a scheduled tick
  rather than one thread per device (2,000 threads is wasteful).
- Each device has a fixed baseline temperature + small Gaussian noise, with a
  low-probability random spike injected occasionally so anomalies actually show up.
- Publishes to Kafka keyed by `deviceId` (keeps a device's events on one partition,
  useful for ordering).
- Scales by running multiple instances (each owns a shard of the 2,000 device IDs)
  or by turning up its own throughput knob — this is what exercises the "can it
  scale" part of the assignment independent of the Flink job's own parallelism.

## 4. Kafka — why and how

- Topic: `iot-readings`, partitioned (e.g. 6-8 partitions) and keyed by `deviceId`.
- Why here specifically: it's the seam between "how fast data arrives" and "how
  fast Flink processes it" — lets us scale producers, brokers, and Flink
  parallelism independently, and gives replay/backpressure handling for free
  instead of building it into the generator.
- Flink source: `KafkaSource` (new unified source API, not the deprecated
  `FlinkKafkaConsumer`), with a `WatermarkStrategy.forBoundedOutOfOrderness(...)`
  on `timestampMillis` for event-time windowing.

## 5. Flink Job — two stdev implementations

Both share the same skeleton:

```
KafkaSource<Reading>
  -> assignTimestampsAndWatermarks(...)
  -> keyBy(Reading::getDeviceId)
  -> window(TumblingEventTimeWindows.of(Time.minutes(1)))
  -> <implementation A or B>
  -> print()
```

The two implementations answer the same question — "what's the mean/stdev of this
device's readings this minute, and which readings exceed 3σ from it" — with
different tradeoffs, and both are kept in the codebase side by side (different
`main()` entry points / job classes) so they can be compared directly.

### Implementation A — naive, two-pass (`SimpleStdDevJob`)

Uses `ProcessWindowFunction`, which buffers every element of the window in Flink
state (`ListState` under the hood) and hands you an `Iterable<Reading>` when the
window fires:

```java
class NaiveAnomalyFn extends ProcessWindowFunction<Reading, String, String, TimeWindow> {
    public void process(String deviceId, Context ctx, Iterable<Reading> elements, Collector<String> out) {
        double sum = 0; int count = 0;
        for (Reading r : elements) { sum += r.temperatureC; count++; }     // pass 1: mean
        double mean = sum / count;

        double sqDiffSum = 0;
        for (Reading r : elements) { sqDiffSum += Math.pow(r.temperatureC - mean, 2); } // pass 2: variance
        double stdev = Math.sqrt(sqDiffSum / count);

        for (Reading r : elements) {                                       // pass 3: flag
            if (count > 1 && Math.abs(r.temperatureC - mean) > 3 * stdev) {
                out.collect(String.format("Device %s, measurement %.1f C, time %d",
                        r.deviceId, r.temperatureC, r.timestampMillis));
            }
        }
    }
}
```

- Straightforward, easy to reason about and test — this is the "understand the
  math first" version.
- Cost: Flink must **buffer every raw reading** in per-key window state until fire
  time, and does 2-3 full passes over that buffer when it fires. At this scale
  (~6,000 readings/device/window × 2,000 devices ≈ 12M buffered elements
  cluster-wide at peak) that's real heap/state pressure — the honest scaling
  limitation of this approach, and the reason implementation B exists.

### Implementation B — incremental / aggregated (`AggregatedStdDevJob`)

Uses a Flink `AggregateFunction` with **Welford's online algorithm** so the
mean/variance are updated incrementally as each reading arrives, rather than
requiring a full materialized buffer to loop over twice:

```java
class WelfordAccumulator {
    long count = 0;
    double mean = 0;
    double m2 = 0; // sum of squared differences from the current mean
}

class WelfordAgg implements AggregateFunction<Reading, WelfordAccumulator, WelfordAccumulator> {
    public WelfordAccumulator createAccumulator() { return new WelfordAccumulator(); }

    public WelfordAccumulator add(Reading r, WelfordAccumulator acc) {
        acc.count++;
        double delta = r.temperatureC - acc.mean;
        acc.mean += delta / acc.count;
        double delta2 = r.temperatureC - acc.mean;
        acc.m2 += delta * delta2;
        return acc;
    }

    public WelfordAccumulator getResult(WelfordAccumulator acc) { return acc; }
    public WelfordAccumulator merge(WelfordAccumulator a, WelfordAccumulator b) { ... } // for session/merging windows
}
```

Each new reading updates `(count, mean, m2)` in O(1) — no second loop over stored
history to get the mean before computing variance.

The remaining wrinkle: to say *which individual readings* were anomalous, we still
need the raw values once the window's final mean/stdev are known at fire time.
Rather than introducing a second stream + join (a real option, but overkill for
this scope — noted in §8 as a future scaling step, not built here), the pragmatic
version combines the AggregateFunction with a `ProcessWindowFunction` via
`window.aggregate(AggregateFunction, ProcessWindowFunction)`, and keeps a small
`List<Reading>` alongside the Welford accumulator to re-check against the final
stats when the window closes:

```java
class WelfordAccumulator {
    long count = 0; double mean = 0; double m2 = 0;
    List<Reading> readings = new ArrayList<>(); // only for the final flag pass
}
```

- Single pass for the *statistics* (the part the assignment specifically calls
  out): mean and variance are correct and final the moment the last reading of the
  window is added, no separate averaging pass.
- Still O(n) space per key per window for the flagging pass (unavoidable if every
  anomalous *individual reading* must be reported) — but computed incrementally
  rather than via nested loops, and cleanly separable from the stats computation
  itself if a future version needs to drop per-reading buffering (e.g. if only
  window-level summaries mattered, this would become true O(1) state — see §8).

### Why keep both

The assignment explicitly asks to demonstrate understanding of both styles. A
is the textbook two-pass definition of stdev; B is the streaming-native way
Flink expects you to think (incremental aggregation instead of re-scanning
buffered data). Comparing them side by side is the point — B is not obviously
"strictly better" here because the per-reading flagging still needs the raw
values, which is a good, honest discussion point for the assignment.

## 6. Output

Console sink (`print()` or a `SinkFunction` wrapping `System.out.println`),
formatted exactly per the spec:

```
Device 455weg75uew, measurement 95.4 C, time 45587456
```

## 7. Raw data persistence + secondary job (optional, per assignment §10)

- Add a second sink off the same Flink job: write every raw `Reading` (not just
  anomalies) to a **file sink** (`FileSink`, rolling JSON/CSV files) acting as the
  "raw data table." A real DB (Postgres via JDBC sink) is a drop-in swap later,
  but a file sink avoids adding a service just to satisfy this optional item.
- Optional second job (`ReplayAnomalyJob`): reads the raw file sink's output (or
  a Kafka "raw-archive" topic if preferred) and re-runs the same anomaly logic,
  printing to console or writing to a new "anomalies" output. This demonstrates:
  - decoupling storage from real-time detection,
  - the ability to backfill/replay historical data through the same detection
    logic (bug fixes or threshold changes can be re-applied retroactively),
  - reuse of the same `WelfordAgg`/`NaiveAnomalyFn` classes against a
    bounded/file source instead of the live Kafka source.
- Marked optional/stretch — not required for the core deliverable to "work."

## 8. Scaling considerations (discussion, not all implemented)

- Flink job parallelism should match Kafka partition count on `iot-readings`;
  `keyBy(deviceId)` guarantees a device's readings are processed by one subtask,
  preserving correctness of per-device windows regardless of parallelism.
- Implementation A's state backend should move to RocksDB (vs. heap) once window
  buffering gets large — spills to disk instead of OOMing.
- Implementation B's state stays small (Welford accumulator only) if the
  per-reading buffering is dropped in favor of the two-stream/broadcast-join
  design mentioned in §5 — worth calling out as the natural next step if this
  needed to scale to much larger windows or devices, but not built now (matches
  "don't over-complicate").
- Enable checkpointing for exactly-once / fault tolerance once this moves past a
  demo.
- Generator B scales horizontally by sharding device IDs across instances.

## 9. Build & Deployment

- **Build tool:** Maven (`pom.xml`), single multi-module-free project — Flink jobs,
  both generators, and shared `Reading` model all in one artifact for simplicity.
  Shade plugin to produce a fat jar for `flink run`.
- **Docker Compose** services:
  - `zookeeper` + `kafka` (or KRaft-mode Kafka, no zookeeper, if preferred)
  - `jobmanager` + `taskmanager` (official `flink` image, job jar mounted in)
  - `generator` (Generator B, continuous — its own image built from the same repo)
- Flink jobs submitted via `flink run -c <MainClass> job.jar` against the
  dockerized cluster, one job class per implementation (A/B) so either can be run
  standalone.

## 10. Testing

- **Unit tests** (JUnit 5), no cluster needed:
  - `WelfordAccumulator`/`WelfordAgg` math vs. a naive reference implementation
    over the same fixed input — assert mean/stdev match within epsilon.
  - Anomaly threshold logic (`|x-mean| > 3*stdev`) with hand-crafted edge cases
    (all-equal readings → stdev 0 → nothing flagged; single spike → flagged;
    count ≤ 1 → skipped).
- **Integration test**: Flink's `MiniClusterWithClientResource`, feed a small
  fixed set of `Reading`s (via Generator A / a `fromElements`/`FileSource`)
  through the real windowed pipeline, collect output into a static test sink
  list, assert the expected anomaly lines are produced — run against **both**
  Implementation A and B with the same input to confirm they agree.

## 11. Run script (`run.sh`)

Single entry point for the full flow:
1. `mvn -q package` — build fat jar.
2. `docker compose up -d zookeeper kafka` — bring up the queue, wait for it to be
   healthy, create the `iot-readings` topic if missing.
3. `docker compose up -d jobmanager taskmanager` — bring up the Flink cluster.
4. Submit the chosen job: `docker exec jobmanager flink run -c <A|B MainClass> /opt/flink/job.jar`
   (script argument selects implementation A or B).
5. Start the generator (script argument selects Generator A file-replay or
   Generator B continuous).
6. `docker compose logs -f taskmanager` (or point at the Flink Web UI) so anomaly
   output is visible immediately.
7. `docker compose down` on exit/interrupt.

## 12. Project layout

```
checkpoint/
├── PLAN.md
├── Readme.md
├── pom.xml
├── docker-compose.yml
├── run.sh
├── src/
│   ├── main/java/com/checkpoint/iot/
│   │   ├── model/Reading.java
│   │   ├── generator/SimpleFileGenerator.java
│   │   ├── generator/ContinuousGenerator.java
│   │   ├── job/SimpleStdDevJob.java        (Implementation A)
│   │   ├── job/AggregatedStdDevJob.java    (Implementation B)
│   │   ├── job/ReplayAnomalyJob.java       (optional, §7)
│   │   └── stats/WelfordAccumulator.java
│   ├── main/resources/sample-data/readings.json
│   └── test/java/com/checkpoint/iot/
│       ├── stats/WelfordAccumulatorTest.java
│       ├── job/SimpleStdDevJobIT.java
│       └── job/AggregatedStdDevJobIT.java
```
