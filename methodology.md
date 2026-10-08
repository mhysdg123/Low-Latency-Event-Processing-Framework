# Benchmark Methodology

## Workload and commands

The standalone `framework_benchmark` compares the SPSC event path inspired by LMAX Disruptor v3 with a bounded mutex and `condition_variable` queue. Both use the same `TelemetryEvent` payload (24 bytes on the recorded host), one producer, one consumer, capacity 4,096, and the same deterministic event values and checksum work. The mutex queue preallocates all slots and applies backpressure by waiting while full. The ring-buffer path uses its registered consumer sequence and existing busy-spin producer wait. No event-path logging or per-event allocation is performed.

Configure a Release build and run the default five repetitions without affinity:

```sh
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release -j2
./build-release/framework_benchmark --output results/benchmark_raw.csv
```

Run the same comparison with producer/consumer pinning enabled by adding `--affinity` and choosing a separate output file, for example:

```sh
./build-release/framework_benchmark --affinity --output results/benchmark_pinned.csv
```

With affinity enabled, the helper reads the process's allowed CPU set using `sched_getaffinity`, chooses two distinct permitted logical CPUs, and prefers CPUs from different SMT sibling groups when topology data is available. It pins the producer thread and the consumer thread with `pthread_setaffinity_np`. If fewer than two CPUs are allowed, or either pin operation fails, the run stops and reports the error; it does not silently continue unpinned. The CSV records the selection, allowed mask, and sibling lists. Without `--affinity`, thread placement remains under the operating system scheduler.

Defaults are 20,000,000 measured events per saturation run, 100,000 measured events per paced-latency run, 20,000 warm-up events, and a paced target of 250,000 events/s. The benchmark alternates which implementation runs first on each repetition. CLI options can change counts, offered rate, repetitions, and output path. Each CSV row is one measured implementation/mode/repetition; saturation rows leave latency percentiles blank.

## Timing and statistics

Warm-up events are produced and fully consumed before the measurement timer starts. Throughput timing begins immediately before the measured producer loop and ends when the consumer has processed the final measured event. Thread shutdown and CSV output happen after the timed interval. Throughput is measured events divided by that elapsed wall time.

Paced latency runs schedule producer events against `steady_clock` deadlines using a short CPU-relax spin. The offered rate is the target schedule; achieved rate is also recorded. A timestamp is written after producer backpressure has cleared and immediately before publication/enqueue. The consumer records a second `steady_clock` timestamp when it receives the event. Thus latency includes queueing and consumer scheduling after publication, but excludes time spent waiting for the producer's next scheduled offer and time blocked by producer-side backpressure. All measured events are sampled, then p50, p95, p99, and p99.9 use nearest-rank percentiles.

The CSV records the median duration of 20,000 back-to-back `steady_clock::now()` pairs in `host_notes`. Each latency sample takes two clock reads; the reported percentiles are not corrected by subtracting the calibration because the calibration also contains loop and clock-resolution effects. The calibration is an indication of timestamp cost, not a precise independent estimate. Saturation mode avoids per-event clock reads.

`cpu_usage_pct` is process CPU time divided by wall time, expressed as a percentage of one logical CPU. It can exceed 100% when producer and consumer run concurrently on separate CPUs. CPU time is collected with `std::clock()`. The paced producer's spin used to maintain the offered rate is included.

## Recorded run

Collected on 2026-10-08 using GCC 13.3.0, C++20, CMake Release defaults (`-O3 -DNDEBUG`), in WSL2 on an Intel Core Ultra 7 251HX host. The WSL kernel reported Linux `6.18.40.1-microsoft-standard-WSL2`; `lscpu` exposed 18 logical CPUs as 18 cores with one thread per core, and the process affinity mask allowed CPUs 0–17. The Microsoft hypervisor was reported. A CPU frequency governor was not exposed in this environment. No CPU pinning was used.

Five repetitions per implementation and mode produced these medians; throughput ranges show the minimum and maximum of the five raw runs:

| Mode | Implementation | Throughput median (range), events/s | p50 ns | p95 ns | p99 ns | p99.9 ns | CPU median, % of one CPU |
|---|---|---:|---:|---:|---:|---:|---:|
| Saturation | Disruptor | 59,849,543 (59,642,695–62,344,263) | — | — | — | — | 203.71 |
| Saturation | Mutex/CV | 8,774,113 (8,513,409–8,970,523) | — | — | — | — | 184.96 |
| Paced, target 250k/s | Disruptor | 250,000 (249,999–250,001) | 131 | 187 | 19,977 | 146,832 | 204.72 |
| Paced, target 250k/s | Mutex/CV | 249,999 (249,996–250,001) | 8,797 | 27,905 | 47,951 | 157,182 | 115.99 |

Both implementations produced matching checksums for every paired run. The latency run's `steady_clock` pair median was 16 ns. The process CPU estimate exceeded 200% in several Disruptor runs despite two benchmark threads; treat these CPU figures as approximate under this virtualized environment. Tail latency varied substantially between repetitions, so the median and raw range matter more than any single run. These are measurements of this specific WSL2 workload, not native-host performance claims.

## Environment and limitations

The recorded stage-three run is inside WSL2, so host scheduling, virtualization, and CPU frequency behavior can affect results, especially tail latency. No CPU affinity was applied in that run. The WSL view reported 18 logical CPUs as 18 cores with one thread per core; sysfs listed CPU 0 and CPU 1 as separate sibling groups (`0` and `1`), so the affinity helper selects them on this machine. The allowed mask was 0–17. The Microsoft hypervisor was reported, and no CPU frequency governor was exposed. The Disruptor uses `BusySpinWaitStrategy`; the baseline uses condition-variable waits. Their consumer wait costs are part of the implementation comparison. Producer pacing itself is an active spin to avoid per-event sleeps and their scheduler jitter; this increases CPU usage and does not model every production arrival process. The 4,096-slot capacity, 24-byte payload, deterministic checksum handler, and single target offered rate are one comparison point, not an exhaustive parameter sweep.

## Affinity comparison

The same Release binary was run five times with affinity off and five times with `--affinity`, using the default workload and separate CSV files `results/affinity_unpinned.csv` and `results/affinity_pinned.csv`. The pinned runs selected producer CPU 0 and consumer CPU 1 from allowed mask 0–17. Their sysfs sibling lists were `0` and `1`; `lscpu` exposed one thread per core in this WSL2 view. All corresponding checksums matched.

| Mode | Implementation | Unpinned throughput median | Pinned throughput median | Unpinned p99 median (range) | Pinned p99 median (range) |
|---|---|---:|---:|---:|---:|
| Saturation | Disruptor | 59,942,856 events/s | 59,025,676 events/s | — | — |
| Saturation | Mutex/CV | 8,931,571 events/s | 11,344,232 events/s | — | — |
| Paced, target 250k/s | Disruptor | 250,000 events/s | 250,001 events/s | 19,342 ns (11,109–28,774) | 12,206 ns (4,938–19,673) |
| Paced, target 250k/s | Mutex/CV | 249,996 events/s | 250,000 events/s | 54,491 ns (42,169–102,881) | 47,742 ns (33,101–63,548) |

These results show the effect for this selected WSL2 CPU pair and workload only. The pinned mutex/CV saturation median was higher, while the Disruptor saturation median was slightly lower; tail latency still varied between repetitions. Do not generalize this result to native Linux or other CPU topologies.

Do not interpret these measurements as a general performance guarantee. Compare runs from this CSV only with their recorded configuration and host metadata. Raw per-run data is in `results/benchmark_raw.csv`.

## Wait strategy comparison

The hybrid strategy is selected with `--wait-strategy hybrid --spin-iterations N`; the default remains `busy_spin`. Each `SequenceBarrier::waitFor` call has a local counter: it performs at most N `cpu_relax` checks and then yields until the requested sequence or an alert becomes available. This local state avoids sharing a mutable spin counter across producer and consumer threads. The existing sequencer calls `producerWait()` without identifying the beginning or end of a backpressure interval, so the hybrid strategy yields on each producer capacity-wait call. The BusySpin implementation is unchanged.

Correctness coverage is in `tests/hybrid_wait_strategy_test.cpp`: it checks an empty ring and prolonged waiting, consumer-first and producer-first startup, a producer filling the ring before consumer startup, 100,000 events across repeated capacity-64 wrap-arounds, exact count and checksum, alert-driven halt while empty, and shutdown after draining. Both correctness tests passed in Release and the hybrid wait test passed under ThreadSanitizer using the WSL2 `setarch x86_64 -R` workaround above.

The BusySpin and hybrid benchmark runs used the same Release binary and workload, five repetitions per implementation/mode, no affinity, and `spin_iterations=1000`. The mutex/CV rows are present in each raw file as a workload check; the table compares the Disruptor rows:

| Mode | Strategy | Throughput median (range), events/s | p50 ns | p95 ns | p99 ns | p99.9 ns | CPU median, % of one CPU |
|---|---|---:|---:|---:|---:|---:|---:|
| Saturation | BusySpin | 58,485,095 (57,427,830–61,572,185) | — | — | — | — | 204.67 |
| Saturation | Hybrid, 1000 spins | 58,022,467 (56,844,555–63,176,650) | — | — | — | — | 204.61 |
| Paced, target 250k/s | BusySpin | 250,001 (250,000–250,002) | 134 | 176 | 2,269 | 111,447 | 204.89 |
| Paced, target 250k/s | Hybrid, 1000 spins | 250,001 (250,001–250,002) | 132 | 174 | 9,888 | 137,554 | 205.19 |

At this offered rate, the measured CPU estimates were effectively unchanged, throughput medians were close, and the hybrid p99/p99.9 latency medians were higher. The hybrid wait reduces active polling after its spin limit during longer empty waits, but this workload did not show a CPU reduction. The paced producer itself spins, and WSL2 adds scheduling noise; these measurements do not establish a general strategy winner. Raw data is in `results/wait_busy_spin.csv` and `results/wait_hybrid_spin_yield.csv`.
