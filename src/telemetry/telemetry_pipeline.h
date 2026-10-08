#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <thread>

#include "disruptor/event_handler.h"
#include "disruptor/event_processor.h"
#include "disruptor/exception_handler.h"
#include "disruptor/ring_buffer.h"
#include "disruptor/sequencer.h"
#include "disruptor/wait_strategies.h"

namespace telemetry
{
    enum class EventType : std::uint8_t
    {
        CpuUsage = 0,
        MemoryUsage = 1,
        RequestLatency = 2,
    };

    struct TelemetryEvent
    {
        std::int64_t timestamp_ns{};
        std::uint32_t server_id{};
        EventType type{};
        std::uint64_t value{};
    };

    struct Aggregate
    {
        std::uint64_t count{};
        std::uint64_t sum{};
        std::uint64_t threshold_violations{};
    };

    struct Result
    {
        std::array<Aggregate, 3> by_type{};
        std::uint64_t total_events{};
        std::uint64_t invalid_events{};
        bool started{};
        bool shutdown{};
    };

    inline constexpr std::uint64_t kEventCount = 1'000'000;
    inline constexpr std::uint64_t kCpuAlertThreshold = 90;
    inline constexpr std::uint64_t kLatencyAlertThresholdMs = 100;
    inline constexpr std::size_t kRingSize = 1024;

    [[nodiscard]] inline constexpr EventType event_type_for(std::uint64_t index) noexcept
    {
        return static_cast<EventType>(index % 3);
    }

    [[nodiscard]] inline constexpr std::uint64_t value_for(std::uint64_t index) noexcept
    {
        const auto sample = index / 3;
        switch (event_type_for(index))
        {
        case EventType::CpuUsage:
            return (sample * 37 + 13) % 101; // percent
        case EventType::MemoryUsage:
            return 1024 + sample % 4096; // MiB
        case EventType::RequestLatency:
            return (sample * 17) % 151; // milliseconds
        }
        return 0;
    }

    [[nodiscard]] inline std::int64_t monotonic_now_ns() noexcept
    {
        using namespace std::chrono;
        return duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count();
    }

    class AggregatingHandler final : public disruptor::EventHandler<TelemetryEvent>
    {
    public:
        explicit AggregatingHandler(Result &result) : result_(result) {}

        void onEvent(TelemetryEvent &event, std::int64_t, bool) override
        {
            if (event.server_id == 0 || event.timestamp_ns < last_timestamp_ns_)
            {
                ++result_.invalid_events;
            }
            last_timestamp_ns_ = event.timestamp_ns;

            auto &aggregate = result_.by_type[static_cast<std::size_t>(event.type)];
            ++aggregate.count;
            aggregate.sum += event.value;
            if ((event.type == EventType::CpuUsage && event.value > kCpuAlertThreshold) ||
                (event.type == EventType::RequestLatency && event.value > kLatencyAlertThresholdMs))
            {
                ++aggregate.threshold_violations;
            }
            ++result_.total_events;
        }

        void onStart() override
        {
            started_.store(true, std::memory_order_release);
        }

        void onShutdown() override
        {
            result_.shutdown = true;
        }

        [[nodiscard]] bool started() const noexcept
        {
            return started_.load(std::memory_order_acquire);
        }

    private:
        Result &result_;
        std::int64_t last_timestamp_ns_{-1};
        std::atomic<bool> started_{false};
    };

    inline Result run_demo(std::uint64_t event_count = kEventCount)
    {
        using namespace disruptor;
        if (event_count == 0)
        {
            throw std::invalid_argument("event_count must be greater than zero");
        }

        BusySpinWaitStrategy wait_strategy;
        SingleProducerSequencer<kRingSize, BusySpinWaitStrategy> sequencer(wait_strategy);
        const auto factory = [] { return TelemetryEvent{}; };
        RingBuffer<TelemetryEvent, kRingSize, decltype(sequencer), decltype(factory)> ring(sequencer, factory);
        auto barrier = sequencer.newBarrier({});

        Result result;
        AggregatingHandler handler(result);
        DefaultExceptionHandler<TelemetryEvent> exception_handler;
        EventProcessor<TelemetryEvent, decltype(ring), decltype(barrier), AggregatingHandler>
            processor(ring, barrier, handler, exception_handler);
        ring.setGatingSequences({&processor.getSequence()});

        std::thread consumer([&processor] { processor.run(); });
        while (!handler.started())
        {
            std::this_thread::yield();
        }

        for (std::uint64_t index = 0; index < event_count; ++index)
        {
            const auto sequence = ring.next();
            auto &event = ring.get(sequence);
            event.timestamp_ns = monotonic_now_ns();
            event.server_id = static_cast<std::uint32_t>(1 + index % 8);
            event.type = event_type_for(index);
            event.value = value_for(index);
            ring.publish(sequence);
        }

        const auto final_sequence = static_cast<std::int64_t>(event_count - 1);
        while (processor.getSequence().get() < final_sequence)
        {
            std::this_thread::yield();
        }
        processor.halt();
        consumer.join();

        result.started = handler.started();
        return result;
    }
}
