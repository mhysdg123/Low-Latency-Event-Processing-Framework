# 源码说明与阅读优先级

本文按当前仓库结构说明源码职责，并给出理解和修改项目时的推荐顺序。项目为 C++20 单生产者/单消费者（SPSC）事件处理原型，设计参考 LMAX Disruptor v3。并发核心应作为一个整体阅读；示例、性能测试和文档位于核心之上。

## 阅读优先级

| 优先级 | 目标 | 建议阅读内容 |
|---|---|---|
| P0：先理解数据流 | 看清事件如何被生产、发布、消费和回收 | `sequence.h` → `sequencer.h` → `ring_buffer.h` → `sequence_barrier.h` → `event_processor.h` |
| P1：理解并发等待 | 掌握等待策略的行为及其性能取舍 | `wait_strategies.h`，再对照 `sequence_barrier.h` 中的调用方式 |
| P2：看实际用法 | 将核心 API 映射到完整生产者/消费者流程 | `main.cpp`、`telemetry/telemetry_pipeline.h`、`telemetry_demo.cpp` |
| P3：掌握正确性边界 | 看清测试覆盖的行为与未覆盖范围 | `tests/telemetry_correctness_test.cpp`、`tests/hybrid_wait_strategy_test.cpp` |
| P4：分析性能结果 | 理解测量方式、比较条件和结果局限 | `benchmark.cpp`、`methodology.md`、`results/*.csv` |
| P5：构建与项目背景 | 查看构建入口、目标配置和总体说明 | `CMakeLists.txt`、`build.sh`、`README.md`、`architecture-notes.md`、`interview-notes.md` |

若要修改并发核心，先阅读 P0 和 P1，并检查对应测试；若只是增加业务事件或汇总逻辑，优先从 telemetry 模块和它的测试入手。benchmark 结果只适用于文档记录的机器、参数和测量方法，不要仅凭一次运行改变核心策略。

## 模块说明

### 并发核心：`src/disruptor/`

- `sequence.h`：封装缓存行对齐的原子序号。序号用于发布进度、表示消费者进度以及限制环形缓冲区的槽位复用；acquire/release 操作参与生产者和消费者之间的数据可见性同步。
- `sequencer.h`：提供 `SingleProducerSequencer`。它负责申请序号、发布游标，并根据 gating sequences 对生产者施加容量背压。实现假定单生产者，不支持多个生产者并发申请。
- `ring_buffer.h`：创建并持有固定容量事件槽，提供按序号访问、申请和发布接口，并将操作委托给 sequencer。事件槽在初始化时创建，正常事件路径复用槽位。
- `sequence_barrier.h`：组合 sequencer 游标、依赖序号和 wait strategy，等待指定序号可用；负责查询已发布上界以及 alert/clear-alert 控制。
- `event_processor.h`：消费者运行循环。它通过 barrier 等待事件，调用 handler，再更新自己的消费序号；`halt()` 使用 barrier alert 通知等待中的循环退出。
- `event_handler.h`：事件处理器回调接口，包含事件、批次和生命周期等回调。
- `exception_handler.h`：事件处理器异常回调接口及默认实现。
- `wait_strategies.h`：包含原有 `BusySpinWaitStrategy` 和本项目添加的 `HybridSpinYieldWaitStrategy`。后者先有限自旋，再让出执行权；其行为由 `SequenceBarrier` 的实际调用语义决定。

核心的主要关系为：应用通过 RingBuffer 申请并填充槽位，sequencer 发布序号；EventProcessor 经 SequenceBarrier 等待已发布数据，处理后推进自己的 Sequence。这个消费序号又作为 gating sequence，防止生产者提前覆盖尚未处理的槽位。多消费者依赖关系由 barrier 的依赖序号表达。

### 示例与 telemetry

- `src/main.cpp`：原始示例入口，演示简单 SPSC 流程和 A/B 两个消费者依赖的 diamond 场景。适合从小型端到端例子理解 API。
- `src/telemetry/telemetry_pipeline.h`：定义 telemetry 事件、确定性事件生成、producer/consumer 执行流程、聚合计数与阈值统计。`run_demo()` 返回结果，避免在事件处理回调中逐条输出。
- `src/telemetry_demo.cpp`：命令行演示入口，调用 `run_demo()` 并在处理结束后输出汇总。

### Benchmark

- `src/benchmark.cpp`：独立 benchmark 程序，包含有界 mutex/condition-variable 队列、SPSC ring-buffer 路径、饱和吞吐和 paced latency 测量、CPU 时间估算、可选 Linux CPU affinity、CLI 参数解析和 CSV 输出。
- `methodology.md`：解释 warm-up、计时边界、时钟开销、延迟分位数、CPU 统计口径、机器环境及测量限制。
- `results/`：不同策略、亲和性设置和基准对照的逐次原始 CSV。汇总指标应与相应配置和原始行一起阅读。

Benchmark 运行入口是 CMake 的 `framework_benchmark` 目标。默认参数会执行完整的多轮 workload，试运行时应使用单独的 `/tmp` 输出路径和较小事件数，避免覆盖已记录数据。

### Tests

- `tests/telemetry_correctness_test.cpp`：验证百万事件处理的事件总数、聚合值、阈值计数和干净退出。
- `tests/hybrid_wait_strategy_test.cpp`：覆盖空队列和长等待、生产者/消费者启动顺序、容量回绕、事件总数/checksum 以及 alert/shutdown 行为。

通过 CTest 运行：

```sh
cmake -S . -B /tmp/c20-framework-release -DCMAKE_BUILD_TYPE=Release
cmake --build /tmp/c20-framework-release -j2
ctest --test-dir /tmp/c20-framework-release --output-on-failure
```

### 构建和说明文档

- `CMakeLists.txt`：设置 C++20，定义 `event_processing_examples`、`telemetry_demo`、`framework_benchmark` 和两个 CTest 测试目标。
- `build.sh`：简化的 CMake 配置与构建入口；需要可复现或隔离的 Release 构建时，建议直接按 README 使用独立构建目录。
- `README.md`：项目定位、功能和快速开始。
- `architecture-notes.md`：原始代码结构与并发 API 的基线审查记录。
- `interview-notes.md`：SPSC、内存序、缓存局部性、CPU 亲和性和等待策略的简要技术说明。
- `development-plan.md`：分阶段目标和验收标准。
- `LICENSE`：项目采用的 MIT license。
