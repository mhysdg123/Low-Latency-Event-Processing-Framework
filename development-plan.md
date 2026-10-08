# C++20 Low-Latency Event Processing Framework — Development Plan (Balanced MVP)

## 0. Objective and boundaries

**Goal:** Build on a prototype implementation inspired by **LMAX Disruptor v3** to create a small, reproducible **C++20 low-latency in-process telemetry event processing and performance evaluation project**.

**Budget:** approximately **14–23 hours**, assuming the upstream project builds normally. The estimate is not a guarantee.

**Core contributions:** (1) reproducible benchmarking against a bounded mutex queue; (2) optional Linux CPU affinity; (3) one configurable hybrid spin/yield wait strategy; (4) minimal telemetry producer/consumer demo and correctness tests.

**Out of scope:** TCP/epoll, RPC, databases, web UI, MPSC/MPMC sequencers, new allocator, full trading system, elaborate telemetry pipeline. Preserve upstream algorithms and public APIs unless an essential, narrowly justified change is required.

**Important:** Inspect the actual repository before coding. The proposed paths, types and API names in this plan are illustrative, not guaranteed to exist. Prefer repository conventions.

## 1. Global Codex rules (copy into AGENTS.md if useful)

1. Read repository structure, build instructions, public interfaces and tests before modifying files.
2. Preserve original RingBuffer, Sequencer, SequenceBarrier and EventProcessor semantics and APIs.
3. Implement only the current stage; do not add unrelated features or refactor broadly.
4. Use C++20/CMake and existing dependencies when possible.
5. Add correctness tests for each feature; run available tests and report exact commands/results.
6. Never fabricate performance measurements. Record raw results and machine/compiler/build metadata.
7. Do not claim lock-free/wait-free behavior without inspecting the implementation.
8. Avoid per-event logging, blocking IO or allocation in the measured hot path.
9. Explain concurrency correctness risks, changes and limitations in each stage.
10. Commit at the end of each verified stage. Keep inherited code and new contributions clearly attributed.

## 2. Development stages

### Stage 1 — Repository audit and baseline (1–2 h)

**Codex prompt:**

> Inspect the current event-processing prototype before making changes. Identify the actual directory structure, public APIs, RingBuffer/Sequencer/SequenceBarrier/EventProcessor interactions, existing wait strategies, build commands and test targets. Build and run the original examples and tests if the environment permits. Report existing failures separately from new issues. Create a minimal extension plan, but do not modify core implementation files. Do not assume that a named class or API exists without checking the code.

**Acceptance criteria:**
- At least one original example builds and runs.
- Record exact build/test commands and baseline results.
- Identify real extension points and existing wait strategies.
- No modifications to concurrency core.

**Deliverable:** `architecture-notes.md`, baseline Git commit.

### Stage 2 — Minimal telemetry demo (2–3 h)

**Business:** one producer emits deterministic synthetic CPU usage, memory usage and request latency events; one consumer aggregates counts/values and detects threshold breaches. No networking, persistence or external service.

**Suggested event fields:** monotonic timestamp, server ID, event type, numeric value. Sample demo thresholds: CPU >90%, request latency >100 ms. These are illustrative only.

**Codex prompt:**

> Implement a minimal in-process telemetry example using the existing project API. Use one producer and one consumer. Events contain a monotonic timestamp, server ID, event type and numeric value. Support CPU usage, memory usage and request latency events. The consumer counts events, aggregates values and counts threshold violations. Use deterministic synthetic event generation. Do not perform console I/O, blocking I/O or dynamic allocation per event in the measured hot path. Add correctness tests for event counts, sums, threshold handling and shutdown. Keep the example independent of networking, storage and external services.

**Acceptance criteria:**
- 1,000,000 events produced and consumed, with exact matching counts.
- Aggregates and alerts match deterministic expected values.
- No per-event printing or avoidable allocation.
- Clean shutdown; run ThreadSanitizer where supported, interpret reports carefully.

**Deliverable:** telemetry demo and correctness tests.

### Stage 3 — Benchmark suite and baseline (3–5 h)

**Codex prompt:**

> Add a standalone benchmark harness comparing the existing Disruptor SPSC event path against a bounded mutex+condition_variable queue with equivalent payloads, capacity and producer/consumer workload. Measure steady-state throughput and per-event end-to-end latency using std::chrono::steady_clock. Preallocate event payloads and latency samples, exclude warm-up from measured statistics, and ensure both implementations fully drain the same number of events. Report p50, p95, p99 and p99.9, total events per second, and configuration. Avoid measuring timestamps with wall-clock time. Separate throughput-saturation tests from paced latency tests. Run multiple repetitions and write raw CSV results. Explicitly document timer overhead, producer pacing, backpressure and benchmark limitations.

**Acceptance criteria:**
- Equal workload, queue capacity, producer/consumer count, payload sizes and backpressure behavior are documented.
- Independent saturation throughput and paced end-to-end latency runs.
- Output events/s, p50/p95/p99/p99.9; at least 5 repetitions for final report.
- Export raw CSV and document test methodology.
- Report clock/timestamp overhead, warm-up and drain behavior.

**Deliverable:** benchmark executable(s), CSV output, `methodology.md`.

### Stage 4 — Optional Linux CPU affinity (2–3 h)

**Codex prompt:**

> Add a small Linux-only CPU affinity helper using pthread_setaffinity_np, with explicit error handling and an option to disable pinning. Discover the CPUs allowed by the current process affinity mask rather than assuming CPU IDs 2 and 3 exist. Pin producer and consumer to two distinct allowed logical CPUs when possible. Keep the existing behavior unchanged by default. Extend the benchmark CLI to compare pinned and unpinned runs. Document CPU topology, SMT sibling relationships, allowed CPU mask, and whether the environment is virtualized. Do not silently fall back when pinning fails.

**Acceptance criteria:**
- Pinning on/off selectable; off is default.
- Uses only CPUs permitted by affinity mask; handles fewer than 2 available CPUs.
- Failures reported explicitly.
- Both pinned and unpinned benchmark runs reproducible.

**Deliverable:** CPU affinity helper and benchmark flags.

### Stage 5 — One HybridSpinYieldWaitStrategy (3–5 h)

**Codex prompt:**

> Inspect the existing wait strategy interface and implement one minimal configurable HybridSpinYieldWaitStrategy compatible with its actual semantics. Spin for a configurable bounded number of iterations, then use std::this_thread::yield while waiting. Preserve the original sequence availability, dependency, shutdown and alert behavior. Do not add sleep, futex or a new sequencer. Use appropriate CPU relaxation only where supported, with a portable fallback. Add tests for empty queues, producer/consumer startup ordering, wrap-around, prolonged waiting and graceful shutdown. Compare CPU consumption, throughput and tail latency against an existing wait strategy. Do not change existing wait strategy behavior.

**Acceptance criteria:**
- Implements the actual upstream strategy contract, not an imagined API.
- Tests for empty queue, wrap-around, start order, prolonged waits, shutdown.
- No lost/duplicated events or deadlock in tested conditions.
- Comparison against upstream strategy includes CPU consumption and p99 latency.

**Deliverable:** hybrid strategy, tests, comparative benchmark.

### Stage 6 — Integration, validation and documentation (3–5 h)

**Codex prompt:**

> Perform a final integration review of the framework extension. Run all available correctness tests, release builds and benchmark configurations. Run ASan/UBSan where applicable; run TSan separately if supported and document any incompatibilities or false positives carefully. Verify that all original examples still build and run. Prepare a concise README covering architecture, build commands, added features, reproducible benchmark methodology, machine/CPU/compiler details, actual measured results, limitations and attribution to the LMAX Disruptor v3 design. Do not invent metrics or describe inherited features as newly implemented. Produce a short interview preparation note explaining SPSC sequencing, acquire/release, cache locality, CPU affinity, wait strategy tradeoffs and latency measurement caveats.

**Acceptance criteria:**
- Original examples and added tests pass; any exceptions clearly documented.
- Release build and benchmark run with reproducible commands.
- Actual CSV and methodology, no fabricated improvements.
- README separates upstream functionality from personal contributions.
- Interview notes explain technical tradeoffs and measurement limits.

**Deliverable:** README, benchmark report, `interview-notes.md`.

## 3. Performance test design

### Test environment

Prefer native Linux x86-64 (Ubuntu 24.04 is one reasonable option). WSL2 is fine for development and preliminary runs, but virtualization and scheduling noise can affect tail-latency results. Record CPU model, physical/logical topology, governor if known, allowed CPU mask, OS/kernel, compiler/version, build type/flags, CPU pinning, workload, payload size, queue capacity, repetitions and test duration.

### Test parameters

- Producer/consumer: 1/1 (SPSC).
- Queue capacities: 1,024 / 4,096 / 65,536 (initial default 4,096).
- Payload sizes: 32 / 64 / 256 bytes (initial default 64).
- Final repetitions: at least 5 per selected comparison; warm-up before measuring.
- Do not exhaustively run every parameter combination initially; use one primary configuration and vary one parameter at a time.
- Test correctness with 1,000,000 events; select sufficient duration/count for stable benchmark runs.
- Use steady_clock for monotonic timestamps; report timestamp cost and limitations. End-to-end latency includes queueing and scheduling effects.

### Comparison matrix

| ID | Queue implementation | CPU affinity | Wait strategy | Purpose |
|---|---|---|---|---|
| A | Bounded mutex + condition_variable | Off | CV | Conventional baseline |
| B | Upstream Disruptor | Off | Existing upstream strategy | Disruptor baseline |
| C | Upstream Disruptor | On | Same existing strategy | Isolate affinity effect |
| D | Extended Disruptor | Off | Hybrid spin/yield | Isolate wait strategy effect |
| E | Extended Disruptor | On | Hybrid spin/yield | Combined configuration |

For each relevant run, record throughput (events/s), p50/p95/p99/p99.9 latency, CPU usage, total event count and errors. Keep saturation-throughput and paced-latency runs separate; do not compare tail latencies measured at different offered loads as if equivalent. Check for producer backpressure, queue backlog and sampling bias. Repeat across runs and report variation, not only best-case values.

### Suggested CSV fields

`run_id,implementation,affinity,wait_strategy,capacity,payload_bytes,producer_count,consumer_count,mode,offered_rate,events,throughput_eps,p50_ns,p95_ns,p99_ns,p999_ns,cpu_usage_pct,compiler,build_flags,host_notes`

### Correctness and diagnostic checks

- Exact produced/consumed counts, deterministic checksums and threshold totals.
- Queue wrap-around, full/empty behavior and clean shutdown.
- CPU affinity failures and insufficient CPU availability.
- Sanitizers: ASan/UBSan and separate TSan if supported; do not treat absence of reports as proof of concurrency correctness.
- No per-event console IO in measured region.
- No unverified performance claims; report when optimizations regress performance.

## 4. Suggested working sequence and commits

1. `chore: document upstream architecture and baseline`
2. `feat: add deterministic telemetry SPSC demo`
3. `bench: add mutex baseline and latency throughput suite`
4. `feat: add optional Linux CPU affinity`
5. `feat: add hybrid spin yield wait strategy`
6. `docs: add reproducible performance report and attribution`

At each stage: inspect -> plan -> implement -> build/test -> review diff -> commit. Avoid requesting Codex to implement all six stages in a single turn.

## 5. Definition of done / stop rule

Stop adding features when: upstream examples run; telemetry demo processes 1M events correctly; affinity and hybrid strategy have independent tests; benchmark exports raw data and p50/p99 + throughput + CPU use; at least 5 repeated measurements are documented; README states environment, methodology, limitations and upstream attribution.

**Resume-ready framing:** *Extended an open-source C++20 Disruptor-based SPSC event processing implementation with a configurable hybrid spin/yield wait strategy, optional Linux CPU affinity, deterministic telemetry workloads, and reproducible latency/throughput benchmarking.* Replace generic wording with real verified results after testing.

**Not a claim:** This is not a production distributed event bus or a trading engine. Original RingBuffer and sequencing design remain upstream work.
