# Ring Buffer Core

This directory contains the event sequencing and processing core used by the C++20 framework. Its design is inspired by LMAX Disruptor v3.

## Components

- `sequence.h`: cache-line-aligned atomic progress sequence.
- `sequencer.h`: `SingleProducerSequencer` claims and publishes sequence numbers and applies capacity backpressure using consumer gating sequences. The current implementation supports one producer.
- `ring_buffer.h`: fixed-capacity event storage and sequence-based access/publication.
- `sequence_barrier.h`: waits for published events and optional consumer dependencies; also carries alert state for shutdown.
- `event_processor.h`: consumer loop that reads available events, invokes the handler, and advances its sequence.
- `event_handler.h` and `exception_handler.h`: processing and error callback interfaces.
- `wait_strategies.h`: busy-spin and configurable spin/yield strategies.

Multiple consumers can read the same published stream. Their progress sequences can gate slot reuse, and barriers can express consumer dependencies. The core does not include a multi-producer sequencer, and this project makes no blanket lock-free or wait-free guarantee.

For the producer-to-consumer flow, the concurrency boundaries, and the current limitations, see the repository's [`source guide`](../../source-guide.md) and [`README`](../../README.md).
