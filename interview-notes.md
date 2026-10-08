# Interview Notes: C++20 Low-Latency Event Processing

## SPSC sequencing

The producer claims the next sequence from `SingleProducerSequencer`, writes the event into the corresponding preallocated RingBuffer slot, then publishes the sequence. The consumer waits through a `SequenceBarrier`, handles each available event in order, and advances its `Sequence` after processing. The consumer sequence also acts as a gating sequence: it tells the producer how far it may advance before a ring slot would be reused while still in use. A sequence barrier can also represent dependencies between consumers, as in the inherited diamond example.

This is an SPSC design. The producer-side claim path relies on a single producer; concurrent producers would require a different sequencer and are not supported by this project.

## Acquire/release and visibility

Publishing an event must make the producer's preceding slot writes visible before the consumer reads that slot. The sequence/cursor publication and observation use acquire/release ordering in the upstream implementation: the producer publishes with release semantics, and the consumer observes availability with acquire semantics. The acquire operation synchronizes with publication, so event data written before publication is visible to the consumer. The consumer similarly publishes its progress so the producer can safely reuse slots only after consumption has advanced.

Relaxed operations can be appropriate for sequence arithmetic that does not transfer ownership or publish data, but weakening the publication/observation edges can expose partially initialized or stale slot contents. Memory ordering must be reasoned about across the actual ownership handoff, not inferred from the word “atomic.”

## Ring buffer and locality

The fixed-size ring stores event slots up front and reuses them by sequence, avoiding per-event queue-node allocation. Sequential slot access can help cache locality and gives bounded storage. It does not guarantee that every event fits in cache: ring capacity, payload size, competing work, and CPU placement all matter. The upstream `Sequence` type uses cache-line alignment/padding to reduce false sharing between frequently updated sequence values; padding consumes space and cannot prevent every source of cache contention.

## Affinity and topology

The optional Linux affinity feature discovers the process's permitted CPUs and pins producer and consumer threads to two distinct choices. It is disabled by default. Pinning can reduce migration and make placement more controlled, but it can also hurt throughput or tail latency if CPUs are oversubscribed, share resources, or are poorly chosen. Logical CPU IDs and SMT relationships are machine-specific. Under WSL2, the guest's reported topology and scheduler behavior may not match the physical host exactly, so pinned results should be interpreted as a configuration-specific observation.

## Wait strategy tradeoffs

Busy spinning checks for work continuously. It can minimize wakeup delay when events arrive frequently, at the cost of consuming CPU while idle. `HybridSpinYieldWaitStrategy` performs a bounded number of checks and then yields, which can let another runnable thread use the CPU during longer waits. Yielding asks the scheduler to run something else; it does not promise a sleep duration or a particular wakeup time.

The hybrid implementation keeps its spin counter local to each wait call. In this upstream interface, the producer's `producerWait()` hook does not identify the beginning and end of one continuous capacity-wait episode, so the hybrid producer path yields on each such call. The benchmark currently compares consumer wait strategies, and the default remains the inherited busy-spin strategy. In the recorded WSL2 workload, hybrid waiting did not reduce measured process CPU and had higher p99 and p99.9 latency medians; this is a result for that offered rate and host, not a universal ranking.

## Latency measurement and interpretation

The benchmark uses `std::chrono::steady_clock` for monotonic scheduling and timestamps. In paced mode, the producer writes a timestamp after backpressure clears and immediately before publish/enqueue; the consumer timestamps receipt. This captures queueing and consumer scheduling after publication, while excluding the producer's wait for its next scheduled offer and time blocked before publication. The benchmark samples every measured event and reports nearest-rank p50, p95, p99, and p99.9.

Each latency sample requires two clock reads, and clock overhead is calibrated separately but not subtracted: the calibration also includes loop and clock-resolution effects. The paced producer actively spins toward its target schedule, which contributes to CPU usage. Saturation throughput is measured separately without per-event timestamps. These choices make the two modes answer different questions; throughput and latency figures should be compared only with matching workload, configuration, and measurement boundaries.

Tail latency is sensitive to scheduling, virtualization, frequency changes, interrupts, warm-up, and sample count. The recorded results came from WSL2 and varied across repetitions. Report the raw runs and environment, avoid conclusions from a single best result, and treat sanitizer runs as checks rather than proofs of correctness.

## Project scope

The project adds a deterministic telemetry workload, correctness tests, benchmark harness, optional CPU affinity, and one hybrid wait strategy. The RingBuffer, sequencer, barrier, processor, and original examples are inherited from the upstream repository. There is no MPMC sequencer, network transport, persistence layer, or claim that the framework is production-ready.
