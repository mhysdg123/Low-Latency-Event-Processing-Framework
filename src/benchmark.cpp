#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <future>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <limits>
#include <numeric>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#include <sys/utsname.h>
#endif

#include "disruptor/event_handler.h"
#include "disruptor/event_processor.h"
#include "disruptor/exception_handler.h"
#include "disruptor/ring_buffer.h"
#include "disruptor/sequencer.h"
#include "disruptor/wait_strategies.h"
#include "telemetry/telemetry_pipeline.h"

namespace
{
    using Clock = std::chrono::steady_clock;
    using telemetry::TelemetryEvent;
    constexpr std::size_t kCapacity = 4096;

    struct Options
    {
        std::uint64_t saturation_events{20'000'000};
        std::uint64_t latency_events{100'000};
        std::uint64_t warmup_events{20'000};
        std::uint64_t offered_rate{250'000};
        std::uint64_t spin_iterations{1000};
        std::uint32_t repetitions{5};
        std::filesystem::path output{"results/benchmark_raw.csv"};
        bool affinity_enabled{false};
        std::string wait_strategy{"busy_spin"};
    };

    struct CpuAffinity
    {
        std::vector<int> allowed_cpus;
        int producer_cpu{-1};
        int consumer_cpu{-1};
        std::string producer_siblings;
        std::string consumer_siblings;
    };

    struct Metrics
    {
        std::uint64_t events{};
        std::uint64_t checksum{};
        double throughput_eps{};
        double cpu_usage_pct{};
        std::uint64_t p50_ns{};
        std::uint64_t p95_ns{};
        std::uint64_t p99_ns{};
        std::uint64_t p999_ns{};
        double elapsed_seconds{};
        std::vector<std::uint64_t> latencies;
    };

    enum class Mode
    {
        Saturation,
        PacedLatency,
    };

    void usage(const char *program)
    {
        std::cout << "Usage: " << program << " [options]\n"
                  << "  --saturation-events N  measured events per saturation run (default 20000000)\n"
                  << "  --latency-events N     measured events per paced run (default 100000)\n"
                  << "  --warmup-events N      excluded warm-up events per run (default 20000)\n"
                  << "  --offered-rate N       paced latency target in events/s (default 250000)\n"
                  << "  --wait-strategy NAME   Disruptor wait strategy: busy_spin or hybrid (default busy_spin)\n"
                  << "  --spin-iterations N    hybrid spin checks before yield (default 1000)\n"
                  << "  --repetitions N        repetitions per implementation and mode (default 5)\n"
                  << "  --affinity             pin producer and consumer to allowed CPUs (default off)\n"
                  << "  --output PATH          raw CSV destination (default results/benchmark_raw.csv)\n";
    }

    std::uint64_t parse_u64(std::string_view value, const char *option)
    {
        std::uint64_t parsed{};
        const auto result = std::from_chars(value.data(), value.data() + value.size(), parsed);
        if (result.ec != std::errc{} || result.ptr != value.data() + value.size())
        {
            throw std::invalid_argument(std::string("invalid value for ") + option);
        }
        return parsed;
    }

    Options parse_options(int argc, char **argv)
    {
        Options options;
        for (int i = 1; i < argc; ++i)
        {
            const std::string_view arg(argv[i]);
            if (arg == "--help" || arg == "-h")
            {
                usage(argv[0]);
                std::exit(0);
            }
            if (arg == "--affinity")
            {
                options.affinity_enabled = true;
                continue;
            }
            if (i + 1 >= argc)
            {
                throw std::invalid_argument(std::string("missing value for ") + std::string(arg));
            }
            const std::string_view value(argv[++i]);
            if (arg == "--saturation-events")
                options.saturation_events = parse_u64(value, "--saturation-events");
            else if (arg == "--latency-events")
                options.latency_events = parse_u64(value, "--latency-events");
            else if (arg == "--warmup-events")
                options.warmup_events = parse_u64(value, "--warmup-events");
            else if (arg == "--offered-rate")
                options.offered_rate = parse_u64(value, "--offered-rate");
            else if (arg == "--spin-iterations")
                options.spin_iterations = parse_u64(value, "--spin-iterations");
            else if (arg == "--wait-strategy")
            {
                options.wait_strategy = value;
                if (options.wait_strategy != "busy_spin" && options.wait_strategy != "hybrid")
                    throw std::invalid_argument("--wait-strategy must be busy_spin or hybrid");
            }
            else if (arg == "--repetitions")
            {
                const auto repetitions = parse_u64(value, "--repetitions");
                if (repetitions > UINT32_MAX)
                    throw std::invalid_argument("--repetitions is too large");
                options.repetitions = static_cast<std::uint32_t>(repetitions);
            }
            else if (arg == "--output")
                options.output = value;
            else
                throw std::invalid_argument(std::string("unknown option: ") + std::string(arg));
        }

        if (options.saturation_events == 0 || options.latency_events == 0 || options.repetitions == 0 || options.offered_rate == 0)
            throw std::invalid_argument("event counts, offered rate, and repetitions must be greater than zero");
        if (options.warmup_events > static_cast<std::uint64_t>(INT64_MAX) ||
            options.latency_events > std::numeric_limits<std::uint64_t>::max() / 1'000'000'000ULL ||
            options.spin_iterations > std::numeric_limits<std::size_t>::max())
            throw std::invalid_argument("warm-up count or paced duration exceeds supported range");
        if (options.saturation_events > static_cast<std::uint64_t>(INT64_MAX) - options.warmup_events ||
            options.latency_events > static_cast<std::uint64_t>(INT64_MAX) - options.warmup_events)
            throw std::invalid_argument("event count plus warm-up exceeds sequence range");
        return options;
    }

    void fill_event(TelemetryEvent &event, std::uint64_t index, std::int64_t timestamp_ns)
    {
        event.timestamp_ns = timestamp_ns;
        event.server_id = static_cast<std::uint32_t>(1 + index % 8);
        event.type = telemetry::event_type_for(index);
        event.value = telemetry::value_for(index);
    }

    std::uint64_t consume_event(const TelemetryEvent &event) noexcept
    {
        return event.value + event.server_id + static_cast<std::uint8_t>(event.type);
    }

    void wait_for_sequence(disruptor::Sequence &sequence, std::int64_t target)
    {
        while (sequence.get() < target)
            std::this_thread::yield();
    }

    void wait_for_count(const std::atomic<std::uint64_t> &count, std::uint64_t target)
    {
        while (count.load(std::memory_order_acquire) < target)
            std::this_thread::yield();
    }

    std::string cpu_list_string(const std::vector<int> &cpus)
    {
        if (cpus.empty())
            return "none";
        std::string result;
        for (std::size_t i = 0; i < cpus.size();)
        {
            const int first = cpus[i];
            int last = first;
            while (i + 1 < cpus.size() && cpus[i + 1] == last + 1)
                last = cpus[++i];
            if (!result.empty())
                result += ',';
            result += std::to_string(first);
            if (last != first)
                result += '-' + std::to_string(last);
            ++i;
        }
        return result;
    }

    std::string read_cpu_siblings(int cpu)
    {
#if defined(__linux__)
        std::ifstream input("/sys/devices/system/cpu/cpu" + std::to_string(cpu) + "/topology/thread_siblings_list");
        std::string siblings;
        if (input >> siblings)
            return siblings;
#endif
        return "unknown";
    }

    std::vector<int> allowed_cpu_ids()
    {
#if defined(__linux__)
        cpu_set_t allowed;
        CPU_ZERO(&allowed);
        if (sched_getaffinity(0, sizeof(allowed), &allowed) != 0)
            throw std::system_error(errno, std::generic_category(), "sched_getaffinity failed");

        std::vector<int> cpus;
        for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu)
        {
            if (CPU_ISSET(cpu, &allowed))
                cpus.push_back(cpu);
        }
        return cpus;
#else
        throw std::runtime_error("CPU affinity discovery is supported only on Linux");
#endif
    }

    CpuAffinity discover_cpu_affinity()
    {
        CpuAffinity affinity;
        affinity.allowed_cpus = allowed_cpu_ids();
        if (affinity.allowed_cpus.size() < 2)
        {
            throw std::runtime_error("--affinity requires at least two CPUs in the process allowed mask; found " +
                                     std::to_string(affinity.allowed_cpus.size()));
        }

        affinity.producer_cpu = affinity.allowed_cpus.front();
        affinity.producer_siblings = read_cpu_siblings(affinity.producer_cpu);
        for (const auto cpu : affinity.allowed_cpus)
        {
            if (cpu != affinity.producer_cpu && read_cpu_siblings(cpu) != affinity.producer_siblings)
            {
                affinity.consumer_cpu = cpu;
                break;
            }
        }
        if (affinity.consumer_cpu < 0)
            affinity.consumer_cpu = affinity.allowed_cpus[1];
        affinity.consumer_siblings = read_cpu_siblings(affinity.consumer_cpu);
        return affinity;
    }

    void pin_current_thread(int cpu)
    {
#if defined(__linux__)
        if (cpu < 0 || cpu >= CPU_SETSIZE)
            throw std::runtime_error("CPU id is outside cpu_set_t range: " + std::to_string(cpu));
        cpu_set_t target;
        CPU_ZERO(&target);
        CPU_SET(cpu, &target);
        const int error = pthread_setaffinity_np(pthread_self(), sizeof(target), &target);
        if (error != 0)
            throw std::system_error(error, std::generic_category(), "pthread_setaffinity_np failed for CPU " + std::to_string(cpu));
#else
        (void)cpu;
        throw std::runtime_error("CPU affinity is supported only on Linux");
#endif
    }

    void pace_to(Clock::time_point start, std::uint64_t index, std::uint64_t rate)
    {
        const auto offset = std::chrono::nanoseconds((index * 1'000'000'000ULL) / rate);
        const auto deadline = start + offset;
        while (Clock::now() < deadline)
            ::cpu_relax();
    }

    class DisruptorHandler final : public disruptor::EventHandler<TelemetryEvent>
    {
    public:
        DisruptorHandler(std::uint64_t warmup, bool collect_latency, std::vector<std::uint64_t> &latencies)
            : warmup_(warmup), collect_latency_(collect_latency), latencies_(latencies) {}

        void onEvent(TelemetryEvent &event, std::int64_t sequence, bool) override
        {
            checksum_ += consume_event(event);
            if (collect_latency_ && static_cast<std::uint64_t>(sequence) >= warmup_)
            {
                const auto now = telemetry::monotonic_now_ns();
                latencies_[static_cast<std::size_t>(static_cast<std::uint64_t>(sequence) - warmup_)] =
                    static_cast<std::uint64_t>(now - event.timestamp_ns);
            }
        }

        void onStart() override
        {
            started_.store(true, std::memory_order_release);
        }

        [[nodiscard]] bool started() const noexcept
        {
            return started_.load(std::memory_order_acquire);
        }

        [[nodiscard]] std::uint64_t checksum() const noexcept
        {
            return checksum_;
        }

    private:
        std::uint64_t warmup_;
        bool collect_latency_;
        std::vector<std::uint64_t> &latencies_;
        std::uint64_t checksum_{};
        std::atomic<bool> started_{false};
    };

    class BoundedMutexQueue
    {
    public:
        explicit BoundedMutexQueue(std::size_t capacity) : slots_(capacity) {}

        template <typename Fill>
        void push(Fill &&fill)
        {
            std::unique_lock lock(mutex_);
            not_full_.wait(lock, [this] { return size_ < slots_.size(); });
            fill(slots_[tail_]);
            tail_ = (tail_ + 1) % slots_.size();
            ++size_;
            lock.unlock();
            not_empty_.notify_one();
        }

        void pop(TelemetryEvent &event)
        {
            std::unique_lock lock(mutex_);
            not_empty_.wait(lock, [this] { return size_ > 0; });
            event = slots_[head_];
            head_ = (head_ + 1) % slots_.size();
            --size_;
            lock.unlock();
            not_full_.notify_one();
        }

    private:
        std::vector<TelemetryEvent> slots_;
        std::mutex mutex_;
        std::condition_variable not_empty_;
        std::condition_variable not_full_;
        std::size_t head_{};
        std::size_t tail_{};
        std::size_t size_{};
    };

    void summarize_latencies(Metrics &metrics)
    {
        if (metrics.latencies.empty())
            return;
        std::sort(metrics.latencies.begin(), metrics.latencies.end());
        const auto value_at = [&metrics](std::uint64_t numerator, std::uint64_t denominator) {
            const auto rank = (metrics.latencies.size() * numerator + denominator - 1) / denominator;
            return metrics.latencies[static_cast<std::size_t>(rank - 1)];
        };
        metrics.p50_ns = value_at(50, 100);
        metrics.p95_ns = value_at(95, 100);
        metrics.p99_ns = value_at(99, 100);
        metrics.p999_ns = value_at(999, 1000);
    }

    template <typename WaitStrategy>
    Metrics run_disruptor(Mode mode, std::uint64_t measured_events, std::uint64_t warmup_events,
                          std::uint64_t offered_rate, const CpuAffinity *affinity,
                          const WaitStrategy &wait_strategy)
    {
        using namespace disruptor;
        const bool collect_latency = mode == Mode::PacedLatency;
        Metrics metrics;
        metrics.events = measured_events;
        if (collect_latency)
            metrics.latencies.resize(static_cast<std::size_t>(measured_events));

        SingleProducerSequencer<kCapacity, WaitStrategy> sequencer(wait_strategy);
        const auto factory = [] { return TelemetryEvent{}; };
        RingBuffer<TelemetryEvent, kCapacity, decltype(sequencer), decltype(factory)> ring(sequencer, factory);
        auto barrier = sequencer.newBarrier({});
        DisruptorHandler handler(warmup_events, collect_latency, metrics.latencies);
        DefaultExceptionHandler<TelemetryEvent> exception_handler;
        EventProcessor<TelemetryEvent, decltype(ring), decltype(barrier), DisruptorHandler>
            processor(ring, barrier, handler, exception_handler);
        ring.setGatingSequences({&processor.getSequence()});

        std::promise<void> consumer_setup;
        auto consumer_ready = consumer_setup.get_future();
        std::thread consumer([&processor, affinity, &consumer_setup] {
            try
            {
                if (affinity)
                    pin_current_thread(affinity->consumer_cpu);
                consumer_setup.set_value();
            }
            catch (...)
            {
                consumer_setup.set_exception(std::current_exception());
                return;
            }
            processor.run();
        });
        try
        {
            consumer_ready.get();
        }
        catch (...)
        {
            consumer.join();
            throw;
        }
        while (!handler.started())
            std::this_thread::yield();

        for (std::uint64_t i = 0; i < warmup_events; ++i)
        {
            const auto sequence = ring.next();
            fill_event(ring.get(sequence), i, 0);
            ring.publish(sequence);
        }
        wait_for_sequence(processor.getSequence(), static_cast<std::int64_t>(warmup_events) - 1);

        const auto pace_start = Clock::now();
        const auto wall_start = Clock::now();
        const auto cpu_start = std::clock();
        for (std::uint64_t i = 0; i < measured_events; ++i)
        {
            if (collect_latency)
                pace_to(pace_start, i, offered_rate);
            const auto sequence = ring.next();
            auto &event = ring.get(sequence);
            fill_event(event, warmup_events + i, collect_latency ? telemetry::monotonic_now_ns() : 0);
            ring.publish(sequence);
        }
        wait_for_sequence(processor.getSequence(), static_cast<std::int64_t>(warmup_events + measured_events) - 1);
        const auto wall_end = Clock::now();
        const auto cpu_end = std::clock();
        processor.halt();
        consumer.join();

        metrics.checksum = handler.checksum();
        metrics.elapsed_seconds = std::chrono::duration<double>(wall_end - wall_start).count();
        metrics.throughput_eps = static_cast<double>(measured_events) / metrics.elapsed_seconds;
        metrics.cpu_usage_pct = 100.0 * static_cast<double>(cpu_end - cpu_start) /
                                static_cast<double>(CLOCKS_PER_SEC) / metrics.elapsed_seconds;
        summarize_latencies(metrics);
        metrics.latencies.clear();
        metrics.latencies.shrink_to_fit();
        return metrics;
    }

    Metrics run_disruptor_selected(Mode mode, std::uint64_t measured_events, std::uint64_t warmup_events,
                                   std::uint64_t offered_rate, const CpuAffinity *affinity,
                                   const Options &options)
    {
        if (options.wait_strategy == "hybrid")
        {
            disruptor::HybridSpinYieldWaitStrategy strategy(static_cast<std::size_t>(options.spin_iterations));
            return run_disruptor(mode, measured_events, warmup_events, offered_rate, affinity, strategy);
        }
        disruptor::BusySpinWaitStrategy strategy;
        return run_disruptor(mode, measured_events, warmup_events, offered_rate, affinity, strategy);
    }

    Metrics run_mutex_queue(Mode mode, std::uint64_t measured_events, std::uint64_t warmup_events,
                            std::uint64_t offered_rate, const CpuAffinity *affinity)
    {
        const bool collect_latency = mode == Mode::PacedLatency;
        Metrics metrics;
        metrics.events = measured_events;
        if (collect_latency)
            metrics.latencies.resize(static_cast<std::size_t>(measured_events));

        BoundedMutexQueue queue(kCapacity);
        std::atomic<std::uint64_t> consumed{0};
        std::uint64_t checksum = 0;
        std::promise<void> consumer_setup;
        auto consumer_ready = consumer_setup.get_future();
        std::thread consumer([&] {
            try
            {
                if (affinity)
                    pin_current_thread(affinity->consumer_cpu);
                consumer_setup.set_value();
            }
            catch (...)
            {
                consumer_setup.set_exception(std::current_exception());
                return;
            }
            TelemetryEvent event;
            const auto total = warmup_events + measured_events;
            for (std::uint64_t index = 0; index < total; ++index)
            {
                queue.pop(event);
                checksum += consume_event(event);
                if (collect_latency && index >= warmup_events)
                {
                    const auto now = telemetry::monotonic_now_ns();
                    metrics.latencies[static_cast<std::size_t>(index - warmup_events)] =
                        static_cast<std::uint64_t>(now - event.timestamp_ns);
                }
                if ((index + 1) % 64 == 0 || index + 1 == warmup_events || index + 1 == total)
                    consumed.store(index + 1, std::memory_order_release);
            }
        });
        try
        {
            consumer_ready.get();
        }
        catch (...)
        {
            consumer.join();
            throw;
        }

        const auto producer_push = [&](std::uint64_t index, bool measure) {
            queue.push([&](TelemetryEvent &slot) {
                fill_event(slot, index, measure ? telemetry::monotonic_now_ns() : 0);
            });
        };
        for (std::uint64_t i = 0; i < warmup_events; ++i)
            producer_push(i, false);
        wait_for_count(consumed, warmup_events);

        const auto pace_start = Clock::now();
        const auto wall_start = Clock::now();
        const auto cpu_start = std::clock();
        for (std::uint64_t i = 0; i < measured_events; ++i)
        {
            if (collect_latency)
                pace_to(pace_start, i, offered_rate);
            producer_push(warmup_events + i, collect_latency);
        }
        wait_for_count(consumed, warmup_events + measured_events);
        const auto wall_end = Clock::now();
        const auto cpu_end = std::clock();
        consumer.join();

        metrics.checksum = checksum;
        metrics.elapsed_seconds = std::chrono::duration<double>(wall_end - wall_start).count();
        metrics.throughput_eps = static_cast<double>(measured_events) / metrics.elapsed_seconds;
        metrics.cpu_usage_pct = 100.0 * static_cast<double>(cpu_end - cpu_start) /
                                static_cast<double>(CLOCKS_PER_SEC) / metrics.elapsed_seconds;
        summarize_latencies(metrics);
        metrics.latencies.clear();
        metrics.latencies.shrink_to_fit();
        return metrics;
    }

    std::string host_notes(const CpuAffinity *affinity)
    {
        std::string details = "Linux";
#if defined(__linux__)
        struct utsname info{};
        if (uname(&info) == 0)
        {
            details = std::string(info.sysname) + " " + info.release;
            if (details.find("microsoft") != std::string::npos || details.find("Microsoft") != std::string::npos)
                details += "; WSL2";
        }
        std::ifstream cpuinfo("/proc/cpuinfo");
        std::string line;
        while (std::getline(cpuinfo, line))
        {
            const auto marker = line.find("model name");
            if (marker != std::string::npos)
            {
                const auto colon = line.find(':', marker);
                if (colon != std::string::npos)
                    details += "; " + line.substr(colon + 2);
                break;
            }
        }
#endif
        details += "; logical_cpus=" + std::to_string(std::thread::hardware_concurrency());
        if (affinity)
        {
            details += "; allowed_cpu_mask=" + cpu_list_string(affinity->allowed_cpus);
            details += "; producer_cpu=" + std::to_string(affinity->producer_cpu) +
                       "(siblings=" + affinity->producer_siblings + ")";
            details += "; consumer_cpu=" + std::to_string(affinity->consumer_cpu) +
                       "(siblings=" + affinity->consumer_siblings + ")";
        }
        else
        {
#if defined(__linux__)
            const auto allowed = allowed_cpu_ids();
            details += "; allowed_cpu_mask=" + cpu_list_string(allowed);
            if (!allowed.empty())
                details += "; cpu" + std::to_string(allowed[0]) + "_siblings=" + read_cpu_siblings(allowed[0]);
            if (allowed.size() > 1)
                details += "; cpu" + std::to_string(allowed[1]) + "_siblings=" + read_cpu_siblings(allowed[1]);
#else
            details += "; allowed_cpu_mask=unavailable; affinity=unsupported";
#endif
        }
        return details;
    }

    std::uint64_t clock_pair_median_ns()
    {
        constexpr std::size_t sample_count = 20'000;
        std::vector<std::uint64_t> samples(sample_count);
        for (auto &sample : samples)
        {
            const auto start = Clock::now();
            const auto end = Clock::now();
            sample = static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
        }
        const auto middle = samples.begin() + samples.size() / 2;
        std::nth_element(samples.begin(), middle, samples.end());
        return *middle;
    }

    std::string csv_quote(const std::string &value)
    {
        std::string quoted = "\"";
        for (const auto ch : value)
        {
            if (ch == '"')
                quoted += '"';
            quoted += ch;
        }
        quoted += '"';
        return quoted;
    }

    void write_header(std::ofstream &out)
    {
        out << "run_id,implementation,affinity,wait_strategy,capacity,payload_bytes,producer_count,consumer_count,"
               "mode,offered_rate,events,throughput_eps,p50_ns,p95_ns,p99_ns,p999_ns,cpu_usage_pct,compiler,"
               "build_flags,host_notes,checksum,elapsed_seconds\n";
    }

    void write_row(std::ofstream &out, const std::string &run_id, const std::string &implementation,
                   const std::string &wait_strategy, Mode mode, std::uint64_t offered_rate, const Metrics &metrics,
                   const std::string &host, const CpuAffinity *affinity)
    {
        const auto mode_name = mode == Mode::Saturation ? "saturation" : "paced_latency";
        const auto affinity_name = affinity ? "on:producer=" + std::to_string(affinity->producer_cpu) +
                                                  ";consumer=" + std::to_string(affinity->consumer_cpu)
                                            : "off";
        out << run_id << ',' << implementation << ',' << csv_quote(affinity_name) << ',' << wait_strategy << ',' << kCapacity << ','
            << sizeof(TelemetryEvent) << ",1,1," << mode_name << ',' << (mode == Mode::Saturation ? 0 : offered_rate)
            << ',' << metrics.events << ',' << std::fixed << std::setprecision(2) << metrics.throughput_eps << ',';
        if (mode == Mode::PacedLatency)
            out << metrics.p50_ns << ',' << metrics.p95_ns << ',' << metrics.p99_ns << ',' << metrics.p999_ns;
        else
            out << ",,,";
        out << ',' << std::setprecision(2) << metrics.cpu_usage_pct << ',' << csv_quote(__VERSION__) << ','
            << csv_quote(DISRUPTOR_BUILD_TYPE "; " DISRUPTOR_BUILD_FLAGS) << ',' << csv_quote(host) << ','
            << metrics.checksum << ',' << std::setprecision(6) << metrics.elapsed_seconds << '\n';
    }

    void write_csv(const Options &options, const std::vector<std::tuple<std::string, std::string, std::string, Mode,
                                                                         std::uint64_t, Metrics>> &rows,
                    const std::string &host, const CpuAffinity *affinity)
    {
        if (!options.output.parent_path().empty())
            std::filesystem::create_directories(options.output.parent_path());
        std::ofstream out(options.output);
        if (!out)
            throw std::runtime_error("cannot open CSV output: " + options.output.string());
        write_header(out);
        for (const auto &[run_id, implementation, strategy, mode, rate, metrics] : rows)
            write_row(out, run_id, implementation, strategy, mode, rate, metrics, host, affinity);
        if (!out)
            throw std::runtime_error("failed while writing CSV output: " + options.output.string());
    }
}

int main(int argc, char **argv)
{
    try
    {
        const auto options = parse_options(argc, argv);
        std::optional<CpuAffinity> affinity;
        if (options.affinity_enabled)
        {
            affinity = discover_cpu_affinity();
            pin_current_thread(affinity->producer_cpu);
        }
        const auto *affinity_config = affinity ? &*affinity : nullptr;
        using Row = std::tuple<std::string, std::string, std::string, Mode, std::uint64_t, Metrics>;
        std::vector<Row> rows;
        rows.reserve(static_cast<std::size_t>(options.repetitions) * 4);
        std::uint64_t run_number = 0;
        auto host = host_notes(affinity_config);
        host += "; steady_clock_pair_median_ns=" + std::to_string(clock_pair_median_ns());
        if (affinity_config)
            std::cout << "CPU affinity enabled: producer=" << affinity_config->producer_cpu
                      << " consumer=" << affinity_config->consumer_cpu
                      << " allowed=" << cpu_list_string(affinity_config->allowed_cpus) << '\n';

        for (std::uint32_t repetition = 1; repetition <= options.repetitions; ++repetition)
        {
            for (const auto mode : {Mode::Saturation, Mode::PacedLatency})
            {
                const auto events = mode == Mode::Saturation ? options.saturation_events : options.latency_events;
                const auto rate = mode == Mode::Saturation ? 0 : options.offered_rate;
                std::optional<std::uint64_t> reference_checksum;
                const std::array<std::string, 2> implementations = repetition % 2 == 1
                                                                        ? std::array<std::string, 2>{"disruptor", "mutex_cv"}
                                                                        : std::array<std::string, 2>{"mutex_cv", "disruptor"};
                for (const auto &implementation : implementations)
                {
                    const auto metrics = implementation == "disruptor"
                                             ? run_disruptor_selected(mode, events, options.warmup_events,
                                                                      options.offered_rate, affinity_config, options)
                                             : run_mutex_queue(mode, events, options.warmup_events,
                                                               options.offered_rate, affinity_config);
                    if (reference_checksum && metrics.checksum != *reference_checksum)
                        throw std::runtime_error("implementations produced different payload checksums");
                    reference_checksum = metrics.checksum;
                    const auto run_id = "run-" + std::to_string(++run_number);
                    rows.emplace_back(run_id,
                                      implementation,
                                      implementation == "disruptor"
                                          ? (options.wait_strategy == "hybrid"
                                                 ? "hybrid_spin_yield:spin_iterations=" + std::to_string(options.spin_iterations)
                                                 : "busy_spin")
                                          : "condition_variable",
                                      mode,
                                      rate,
                                      metrics);
                    std::cout << run_id << ' ' << implementation << ' '
                              << (mode == Mode::Saturation ? "saturation" : "paced_latency")
                              << " events/s=" << std::fixed << std::setprecision(2) << metrics.throughput_eps;
                    if (mode == Mode::PacedLatency)
                        std::cout << " p99_ns=" << metrics.p99_ns;
                    std::cout << " cpu_pct=" << metrics.cpu_usage_pct << '\n';
                }
            }
        }

        write_csv(options, rows, host, affinity_config);
        std::cout << "Wrote " << rows.size() << " raw runs to " << options.output << '\n';
        return 0;
    }
    catch (const std::exception &exception)
    {
        std::cerr << "benchmark error: " << exception.what() << '\n';
        return 2;
    }
}
