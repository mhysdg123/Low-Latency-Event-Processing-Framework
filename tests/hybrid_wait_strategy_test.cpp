#include <atomic>
#include <cstddef>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <thread>
#include <vector>

#include "disruptor/event_handler.h"
#include "disruptor/event_processor.h"
#include "disruptor/exception_handler.h"
#include "disruptor/ring_buffer.h"
#include "disruptor/sequencer.h"
#include "disruptor/wait_strategies.h"

namespace
{
    using namespace std::chrono_literals;

    struct Event
    {
        std::uint64_t value{};
    };

    struct RunResult
    {
        std::uint64_t count{};
        std::uint64_t sum{};
        bool started{};
        bool shutdown{};
    };

    class Handler final : public disruptor::EventHandler<Event>
    {
    public:
        explicit Handler(RunResult &result) : result_(result) {}

        void onEvent(Event &event, std::int64_t, bool) override
        {
            ++result_.count;
            result_.sum += event.value;
        }

        void onStart() override
        {
            started_.store(true, std::memory_order_release);
        }

        void onShutdown() override
        {
            result_.shutdown = true;
        }

        bool started() const
        {
            return started_.load(std::memory_order_acquire);
        }

    private:
        RunResult &result_;
        std::atomic<bool> started_{false};
    };

    class ObservedHybridStrategy
    {
    public:
        explicit ObservedHybridStrategy(std::size_t spin_iterations) : strategy_(spin_iterations) {}

        template <typename Barrier>
        std::int64_t waitFor(std::int64_t sequence, const disruptor::Sequence &cursor,
                             const std::vector<disruptor::Sequence *> &dependents, Barrier &barrier) const
        {
            return strategy_.waitFor(sequence, cursor, dependents, barrier);
        }

        void signalAllWhenBlocking() const noexcept
        {
            strategy_.signalAllWhenBlocking();
        }

        void producerWait() const noexcept
        {
            producer_waited_.store(true, std::memory_order_release);
            strategy_.producerWait();
        }

        bool producer_waited() const noexcept
        {
            return producer_waited_.load(std::memory_order_acquire);
        }

    private:
        disruptor::HybridSpinYieldWaitStrategy strategy_;
        mutable std::atomic<bool> producer_waited_{false};
    };

    bool wait_for(const auto &predicate, std::chrono::milliseconds timeout = 5000ms)
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (!predicate())
        {
            if (std::chrono::steady_clock::now() >= deadline)
                return false;
            std::this_thread::yield();
        }
        return true;
    }

    template <std::size_t Capacity>
    bool run_case(bool producer_first, bool prolonged_empty_wait, std::uint64_t event_count)
    {
        using namespace disruptor;
        ObservedHybridStrategy wait_strategy(256);
        SingleProducerSequencer<Capacity, ObservedHybridStrategy> sequencer(wait_strategy);
        const auto factory = [] { return Event{}; };
        RingBuffer<Event, Capacity, decltype(sequencer), decltype(factory)> ring(sequencer, factory);
        auto barrier = sequencer.newBarrier({});
        RunResult result;
        Handler handler(result);
        DefaultExceptionHandler<Event> exception_handler;
        EventProcessor<Event, decltype(ring), decltype(barrier), Handler>
            processor(ring, barrier, handler, exception_handler);
        ring.setGatingSequences({&processor.getSequence()});

        std::thread consumer;
        if (!producer_first)
        {
            consumer = std::thread([&] { processor.run(); });
            if (!wait_for([&] { return handler.started(); }))
            {
                processor.halt();
                consumer.join();
                std::cerr << "FAIL: consumer did not start\n";
                return false;
            }
            if (prolonged_empty_wait)
            {
                std::this_thread::sleep_for(20ms);
                if (processor.getSequence().get() != -1)
                {
                    processor.halt();
                    consumer.join();
                    std::cerr << "FAIL: empty ring advanced consumer sequence\n";
                    return false;
                }
            }
        }

        std::atomic<std::uint64_t> published{0};
        bool producer_filled_before_consumer = !producer_first;
        std::thread producer([&] {
            for (std::uint64_t i = 0; i < event_count; ++i)
            {
                const auto sequence = ring.next();
                ring.get(sequence).value = i + 1;
                ring.publish(sequence);
                published.store(i + 1, std::memory_order_release);
            }
        });

        if (producer_first)
        {
            producer_filled_before_consumer =
                wait_for([&] {
                    return published.load(std::memory_order_acquire) >= Capacity && wait_strategy.producer_waited();
                });
            consumer = std::thread([&] { processor.run(); });
            if (!wait_for([&] { return handler.started(); }))
            {
                processor.halt();
                producer.join();
                consumer.join();
                std::cerr << "FAIL: consumer did not start after producer\n";
                return false;
            }
            if (!producer_filled_before_consumer)
                std::cerr << "FAIL: producer did not fill ring before consumer startup\n";
        }

        producer.join();
        const auto final_sequence = static_cast<std::int64_t>(event_count - 1);
        if (!wait_for([&] { return processor.getSequence().get() >= final_sequence; }))
        {
            processor.halt();
            consumer.join();
            std::cerr << "FAIL: consumer did not drain all published events\n";
            return false;
        }
        processor.halt();
        consumer.join();

        result.started = handler.started();
        const auto expected_sum = event_count * (event_count + 1) / 2;
        if (result.count != event_count || result.sum != expected_sum || !result.started || !result.shutdown)
        {
            std::cerr << "FAIL: count/sum/start/shutdown=" << result.count << '/' << result.sum << '/'
                      << result.started << '/' << result.shutdown << " expected=" << event_count << '/' << expected_sum << "\n";
            return false;
        }
        return producer_filled_before_consumer;
    }

    bool halt_while_empty()
    {
        using namespace disruptor;
        HybridSpinYieldWaitStrategy wait_strategy(0);
        SingleProducerSequencer<8, HybridSpinYieldWaitStrategy> sequencer(wait_strategy);
        const auto factory = [] { return Event{}; };
        RingBuffer<Event, 8, decltype(sequencer), decltype(factory)> ring(sequencer, factory);
        auto barrier = sequencer.newBarrier({});
        RunResult result;
        Handler handler(result);
        DefaultExceptionHandler<Event> exception_handler;
        EventProcessor<Event, decltype(ring), decltype(barrier), Handler>
            processor(ring, barrier, handler, exception_handler);
        ring.setGatingSequences({&processor.getSequence()});
        std::thread consumer([&] { processor.run(); });
        const bool started = wait_for([&] { return handler.started(); });
        if (started)
            std::this_thread::sleep_for(20ms);
        processor.halt();
        consumer.join();
        result.started = handler.started();
        if (!started || result.count != 0 || !result.shutdown)
        {
            std::cerr << "FAIL: halt did not stop a consumer waiting on an empty ring\n";
            return false;
        }
        return true;
    }
}

int main()
{
    bool passed = true;
    passed &= run_case<64>(false, true, 100'000); // empty/prolonged wait and repeated wrap-around
    passed &= run_case<64>(true, false, 100'000); // producer fills ring before consumer starts
    passed &= halt_while_empty();
    if (passed)
        std::cout << "PASS: hybrid wait, empty/prolonged wait, startup ordering, wrap-around, and shutdown\n";
    return passed ? 0 : 1;
}
