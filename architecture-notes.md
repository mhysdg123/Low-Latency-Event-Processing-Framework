# Architecture Notes and Baseline

## Repository and baseline

The repository is a small header-only C++20 project. `src/main.cpp` contains the only executable entry point and demonstrates a single-producer/single-consumer (SPSC) flow plus a diamond consumer dependency graph. The implementation is under `src/disruptor/`.

The checked-out commit is `09951cb` (`update with exception handler and docs, and minor bugs in example`); the working tree was clean during this audit. The user reports that the project has already built and run successfully in this environment, so build and runtime checks were intentionally skipped for this stage.

The historical baseline was built with `./build.sh` (which ran `cmake -S . -B build` and `cmake --build build`) and its example executable. It defined one example target from `src/main.cpp` and required C++20. That baseline had no test directory, test framework, or CTest target. The current framework adds separate examples, benchmark, and correctness-test targets.

## Actual public types and APIs

- `Sequence` (`sequence.h`) is a cache-line-aligned atomic `int64_t` counter, initialized to `-1`. `get()` uses acquire load; `set()` uses release store. It also provides increment and compare-and-set operations.
- `SingleProducerSequencer<N, WaitStrategy>` (`sequencer.h`) claims one or more sequence numbers with `next(n)`, publishes the final claimed sequence with `publish(sequence)`, tracks a cursor, and throttles producer wrap-around against registered gating sequences. `N` must be a power of two. No multi-producer sequencer is implemented.
- `RingBuffer<T, N, Sequencer, EventFactory>` (`ring_buffer.h`) initializes its fixed `std::array` using a factory, and exposes `next`, `get`/`get_ptr`, `publish`, gating-sequence setup, and cursor access. It holds a reference to the sequencer.
- `SequenceBarrier<Sequencer, WaitStrategy>` (`sequence_barrier.h`) waits for a requested sequence, considers the cursor and dependent sequences, delegates waiting to the strategy, asks the sequencer for the highest published sequence, and supports alert/clear-alert shutdown signaling.
- `EventProcessor<T, DataProvider, SequenceBarrier, EventHandler, ExceptionHandlerType>` (`event_processor.h`) runs a consumer loop on a caller-managed thread, reads events through the data provider, invokes handler callbacks in batches, and advances its own `Sequence`. `halt()` alerts its barrier to wake the loop.
- `EventHandler<T>` (`event_handler.h`) requires `onEvent` and provides optional batch, lifecycle, timeout, and sequence-callback hooks. `ExceptionHandler<T>` and `DefaultExceptionHandler<T>` (`exception_handler.h`) define event/start/shutdown exception callbacks.
- `SequencerConcept` and `WaitStrategyConcept` describe template requirements. A wait strategy must support `signalAllWhenBlocking()` and `producerWait()`; `SequenceBarrier` additionally calls its `waitFor(...)` method.

## Producer-to-consumer interaction

The application creates a wait strategy and sequencer, then a ring buffer that refers to the sequencer. A producer calls `RingBuffer::next()`, writes to `get(sequence)`, and calls `publish(sequence)`. The single-producer sequencer publishes by release-storing its cursor. Consumers run `EventProcessor::run()` on application-created threads. Each processor asks its barrier to `waitFor(nextSequence)`, reads available events from the ring buffer, invokes `onEvent`, then release-stores its consumed sequence. The acquire loads in `Sequence::get()` make published event writes visible to consumers and consumed progress visible to the producer's gating check.

The producer must register the sequences that protect slots from reuse. In the simple example, the sole consumer sequence gates the ring. In the diamond example, A and B consume independently from the ring; C's barrier depends on both A and B, while C's sequence gates ring reuse. This ensures the producer does not overwrite a slot until the downstream consumer has passed it.

## Existing wait strategy

Only `BusySpinWaitStrategy` is implemented. It repeatedly checks the minimum cursor/dependent sequence, checks barrier alerts, and uses `cpu_relax()` while waiting. On x86-64 it uses `_mm_pause`, on AArch64 it uses `__yield`, and other platforms yield the thread. The same strategy's `producerWait()` is used when the producer reaches the ring's wrap point before consumers have advanced. `signalAllWhenBlocking()` is a no-op because this strategy does not block.

## Extension points and limitations

The narrow extension point for a hybrid consumer wait strategy is the wait-strategy type passed to `SingleProducerSequencer`, `SequenceBarrier`, and their concepts. It must preserve barrier alert checks and the existing return semantics: return the observed available sequence, which `SequenceBarrier` then bounds using `getHighestPublishedSequence`. Producer-side backpressure has a separate `producerWait()` hook, so adding a new strategy also affects how the producer waits at capacity. Existing behavior can remain the default by continuing to instantiate `BusySpinWaitStrategy` in current examples.

The telemetry demo can use the existing event factory, `RingBuffer`, one `EventProcessor`, and a concrete handler without changing the concurrency core. A standalone benchmark can compare those components with a separately implemented bounded mutex/CV queue. Linux affinity can be isolated in optional thread setup code outside the Disruptor core.

Current scope is narrower than some README wording: the code has one producer sequencer, one busy-spin strategy, and examples, but no MPMC sequencer, DSL, benchmark harness, automated tests, or separate thread-management layer. The example performs console output in event callbacks, so it is illustrative and unsuitable as-is for measured hot-path benchmarking. `EventProcessor` also stores an exception handler by reference; callers must keep that handler alive for the processor's lifetime.

## Minimal extension plan

1. Add a deterministic telemetry event and one producer/consumer example using the current public API; verify event totals, aggregates, thresholds, and shutdown.
2. Add a standalone benchmark with equivalent SPSC workloads for the Disruptor and a bounded mutex/CV queue; separate saturation throughput from paced latency and write raw CSV.
3. Add optional Linux affinity in benchmark thread setup, discovering CPUs allowed to the process and reporting failures explicitly.
4. Add one bounded spin-then-yield wait strategy behind the existing wait-strategy requirements, with focused wait, wrap-around, start-order, and shutdown checks.
5. Document commands, environment, measured results, methodology, limitations, and upstream attribution after collecting real data.

No concurrency-core implementation files were modified during this audit.
