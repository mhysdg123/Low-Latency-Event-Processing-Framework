#include <array>
#include <cstdint>
#include <iostream>

#include "telemetry/telemetry_pipeline.h"

namespace
{
    struct ExpectedAggregate
    {
        std::uint64_t count{};
        std::uint64_t sum{};
        std::uint64_t alerts{};
    };

    std::array<ExpectedAggregate, 3> expected_for(std::uint64_t events)
    {
        std::array<ExpectedAggregate, 3> expected{};
        for (std::uint64_t index = 0; index < events; ++index)
        {
            const auto type = static_cast<std::size_t>(index % 3);
            const auto sample = index / 3;
            std::uint64_t value = 0;
            if (type == 0)
            {
                value = (sample * 37 + 13) % 101;
                expected[type].alerts += value > 90;
            }
            else if (type == 1)
            {
                value = 1024 + sample % 4096;
            }
            else
            {
                value = (sample * 17) % 151;
                expected[type].alerts += value > 100;
            }
            ++expected[type].count;
            expected[type].sum += value;
        }
        return expected;
    }

    bool check(bool condition, const char *message)
    {
        if (!condition)
        {
            std::cerr << "FAIL: " << message << '\n';
            return false;
        }
        return true;
    }
}

int main()
{
    constexpr auto event_count = telemetry::kEventCount;
    const auto result = telemetry::run_demo(event_count);
    const auto expected = expected_for(event_count);
    bool passed = true;

    passed &= check(result.total_events == event_count, "consumer processed exactly one million events");
    passed &= check(result.invalid_events == 0, "all timestamps and server IDs are valid");
    passed &= check(result.started, "consumer started before production");
    passed &= check(result.shutdown, "consumer shut down cleanly after draining");
    for (std::size_t i = 0; i < expected.size(); ++i)
    {
        passed &= check(result.by_type[i].count == expected[i].count, "per-type count matches expected value");
        passed &= check(result.by_type[i].sum == expected[i].sum, "per-type sum matches expected value");
        passed &= check(result.by_type[i].threshold_violations == expected[i].alerts,
                        "threshold violation count matches expected value");
    }

    if (passed)
    {
        std::cout << "PASS: 1,000,000 events, aggregates, thresholds, and shutdown verified\n";
    }
    return passed ? 0 : 1;
}
