# C++20 Low-Latency Event Processing Framework

An experimental C++20 framework for in-process, low-latency event processing, inspired by the design of **LMAX Disruptor v3**. It includes a deterministic telemetry pipeline, reproducible benchmarks, optional Linux CPU affinity, and a hybrid spin/yield wait strategy.

The framework supports a single producer with one or more consumers, including consumer dependency graphs. Its sequencer requires a single producer; MPMC sequencing is not implemented. The project makes no blanket lock-free or wait-free guarantee.

## Project contents

- `src/disruptor/`: ring buffer, sequencing, barriers, event processing, and wait strategy modules.
- `src/telemetry/` and `src/telemetry_demo.cpp`: deterministic one-million-event telemetry example.
- `src/benchmark.cpp`: saturation-throughput and paced-latency comparison with a bounded mutex/condition-variable queue.
- `tests/`: telemetry correctness and hybrid wait-strategy tests.
- Optional Linux CPU affinity (`--affinity`), disabled by default, and `HybridSpinYieldWaitStrategy`.
- [`methodology.md`](methodology.md), [`results/`](results/), and [`interview-notes.md`](interview-notes.md): measured results, raw benchmark runs, and technical notes.

## Build and test

Requirements: CMake 3.10 or newer and a C++20 compiler.

```sh
cmake -S . -B /tmp/c20-framework-release -DCMAKE_BUILD_TYPE=Release
cmake --build /tmp/c20-framework-release -j2
ctest --test-dir /tmp/c20-framework-release --output-on-failure
```

Run the inherited examples or the added telemetry demo:

```sh
/tmp/c20-framework-release/event_processing_examples
/tmp/c20-framework-release/telemetry_demo
```

Run a benchmark configuration and write raw output outside the checked-in results:

```sh
/tmp/c20-framework-release/framework_benchmark \
  --output /tmp/framework-benchmark.csv
/tmp/c20-framework-release/framework_benchmark \
  --wait-strategy hybrid --spin-iterations 1000 \
  --output /tmp/framework-hybrid.csv
/tmp/c20-framework-release/framework_benchmark \
  --affinity --output /tmp/framework-affinity.csv
```

Default benchmark runs use five repetitions, 20 million saturation events and 100,000 paced-latency events per run. See [`methodology.md`](methodology.md) for measurement boundaries, environment details, actual results, and limitations. Results were collected in WSL2 and are not general performance guarantees.

## Provenance

The framework follows design concepts from LMAX Disruptor v3 and is distributed under the MIT license; see [`LICENSE`](LICENSE). The license retains the original copyright notice for inherited code and identifies the framework extensions maintained here.

## 中文说明

这是一个使用 C++20 编写的进程内低延迟事件处理框架，设计参考 **LMAX Disruptor v3**。项目包含确定性遥测流水线、可复现的基准测试、可选的 Linux CPU 亲和性设置，以及混合自旋/让出等待策略。

框架支持单生产者和一个或多个消费者，也支持消费者依赖关系图。sequencer 要求单生产者，尚未实现 MPMC；项目不对整体实现作 lock-free 或 wait-free 保证。

### 项目内容

- `src/disruptor/`：环形缓冲区、序号、sequencer、barrier、事件处理器和等待策略等核心模块。
- `src/telemetry/` 与 `src/telemetry_demo.cpp`：确定性遥测示例，处理一百万条事件并输出聚合结果。
- `src/benchmark.cpp`：比较饱和吞吐量和定速延迟，并与有界 mutex/condition-variable 队列对照。
- `tests/`：遥测正确性测试和混合等待策略测试。
- 可选 Linux CPU 亲和性（默认关闭，使用 `--affinity` 启用）以及 `HybridSpinYieldWaitStrategy`。
- [`methodology.md`](methodology.md)、[`results/`](results/) 和 [`interview-notes.md`](interview-notes.md)：测试方法、逐次原始数据和技术说明。

### 构建与测试

依赖 CMake 3.10 或更新版本，以及支持 C++20 的编译器。在项目根目录执行：

```sh
cmake -S . -B /tmp/c20-framework-release -DCMAKE_BUILD_TYPE=Release
cmake --build /tmp/c20-framework-release -j2
ctest --test-dir /tmp/c20-framework-release --output-on-failure
```

运行示例和遥测程序：

```sh
/tmp/c20-framework-release/event_processing_examples
/tmp/c20-framework-release/telemetry_demo
```

运行基准测试并将输出写入临时文件，以免覆盖仓库中保存的结果：

```sh
/tmp/c20-framework-release/framework_benchmark \
  --output /tmp/framework-benchmark.csv
/tmp/c20-framework-release/framework_benchmark \
  --wait-strategy hybrid --spin-iterations 1000 \
  --output /tmp/framework-hybrid.csv
/tmp/c20-framework-release/framework_benchmark \
  --affinity --output /tmp/framework-affinity.csv
```

基准测试默认运行 5 轮；每轮包含 2,000 万条饱和吞吐事件和 10 万条定速延迟事件。测量边界、环境信息、实际结果与局限请参阅 [`methodology.md`](methodology.md)。现有结果来自 WSL2，只能说明对应环境和配置下的表现，不代表普遍性能保证。

### 设计来源与许可

本框架参考 LMAX Disruptor v3 的设计理念，使用 MIT 许可证，详见 [`LICENSE`](LICENSE)。许可证保留了基础代码原有的版权声明，并注明本项目扩展的维护者。
