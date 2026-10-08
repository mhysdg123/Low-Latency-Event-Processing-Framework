/**
 * @file wait_strategies.h
 * @brief Defines wait strategies for the disruptor pattern.
 */

#pragma once

#include <algorithm>
#include <cstddef>
#include <vector>
#include <thread>
#include <limits>
#include "sequence.h"

#if defined(__aarch64__)
#include <arm_acle.h>
inline void cpu_relax()
{
    __yield();
}
#elif defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
inline void cpu_relax()
{
    _mm_pause();
}
#else
inline void cpu_relax()
{
    std::this_thread::yield();
}
#endif

namespace disruptor
{

    /**
     * @brief Gets the minimum sequence from cursor and dependents.
     *
     * @param cursor The cursor.
     * @param dependents The dependents.
     * @param minimum Initial minimum.
     * @return The minimum sequence.
     */
    [[nodiscard]] inline int64_t dependents_get(
        const Sequence &cursor,
        const std::vector<Sequence *> &dependents,
        int64_t minimum = std::numeric_limits<int64_t>::max())
    {
        if (dependents.empty())
        {
            return cursor.get();
        }
        for (const auto *seq : dependents)
        {
            if (seq)
            {
                minimum = std::min(minimum, seq->get());
            }
        }
        return minimum;
    }

    /**
     * @brief Concept for wait strategies.
     */
    template <typename W>
    concept WaitStrategyConcept = requires(W w, int64_t seq, Sequence cursor, std::vector<Sequence *> dependents) {
        { w.signalAllWhenBlocking() };
        { w.producerWait() };
    };

    /**
     * @brief Busy spin wait strategy.
     */
    class BusySpinWaitStrategy
    {
    public:
        /**
         * @brief Waits for a sequence.
         *
         * @tparam Barrier Barrier type.
         * @param sequence Sequence to wait for.
         * @param cursor Cursor.
         * @param dependents Dependents.
         * @param barrier Barrier.
         * @return Available sequence.
         */
        // returns sequence available, possibly larger than requested sequence
        template <typename Barrier>
        int64_t waitFor(
            int64_t sequence,
            const Sequence &cursor,
            const std::vector<Sequence *> &dependents,
            Barrier &barrier) const
        {
            int64_t available_sequence;
            while ((available_sequence = dependents_get(cursor, dependents)) < sequence)
            {
                barrier.checkAlert();
                cpu_relax();
            }
            return available_sequence;
        }

        /**
         * @brief Signals all when blocking.
         */
        void signalAllWhenBlocking() const {}

        /**
         * @brief Producer wait.
         */
        void producerWait() const noexcept
        {
            cpu_relax();
        }
    };

    /**
     * @brief Spins for a bounded number of checks, then yields while waiting for sequences.
     *
     * Consumer wait counters are local to waitFor(), so the strategy can be shared by
     * producer and consumer threads without mutable cross-thread state. Producer-side
     * capacity waits yield on each producerWait() call because that hook has no signal
     * marking the start or end of a backpressure episode.
     */
    class HybridSpinYieldWaitStrategy
    {
    public:
        explicit HybridSpinYieldWaitStrategy(std::size_t spinIterations = 1000) noexcept
            : spinIterations_(spinIterations) {}

        template <typename Barrier>
        int64_t waitFor(
            int64_t sequence,
            const Sequence &cursor,
            const std::vector<Sequence *> &dependents,
            Barrier &barrier) const
        {
            std::size_t spins = 0;
            int64_t available_sequence;
            while ((available_sequence = dependents_get(cursor, dependents)) < sequence)
            {
                barrier.checkAlert();
                if (spins < spinIterations_)
                {
                    ::cpu_relax();
                    ++spins;
                }
                else
                {
                    std::this_thread::yield();
                }
            }
            return available_sequence;
        }

        void signalAllWhenBlocking() const noexcept {}

        void producerWait() const noexcept
        {
            std::this_thread::yield();
        }

        [[nodiscard]] std::size_t spinIterations() const noexcept
        {
            return spinIterations_;
        }

    private:
        std::size_t spinIterations_;
    };

} // namespace disruptor
