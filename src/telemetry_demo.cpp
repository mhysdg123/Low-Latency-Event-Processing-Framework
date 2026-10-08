#include <iostream>

#include "telemetry/telemetry_pipeline.h"

namespace
{
    const char *name(telemetry::EventType type)
    {
        switch (type)
        {
        case telemetry::EventType::CpuUsage:
            return "cpu_usage";
        case telemetry::EventType::MemoryUsage:
            return "memory_usage";
        case telemetry::EventType::RequestLatency:
            return "request_latency";
        }
        return "unknown";
    }
}

int main()
{
    const auto result = telemetry::run_demo();
    std::cout << "Consumed " << result.total_events << " telemetry events\n";
    for (std::size_t i = 0; i < result.by_type.size(); ++i)
    {
        const auto type = static_cast<telemetry::EventType>(i);
        const auto &aggregate = result.by_type[i];
        std::cout << name(type) << ": count=" << aggregate.count
                  << " sum=" << aggregate.sum
                  << " threshold_violations=" << aggregate.threshold_violations << '\n';
    }
    std::cout << "invalid_events=" << result.invalid_events
              << " clean_shutdown=" << std::boolalpha << result.shutdown << '\n';
    return result.total_events == telemetry::kEventCount && result.invalid_events == 0 && result.shutdown ? 0 : 1;
}
