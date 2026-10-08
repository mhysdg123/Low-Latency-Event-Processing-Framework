cmake_minimum_required(VERSION 3.10)
project(c20_low_latency_event_processing)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_executable(event_processing_examples src/main.cpp)

target_include_directories(event_processing_examples PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)

add_executable(telemetry_demo src/telemetry_demo.cpp)
target_include_directories(telemetry_demo PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)

string(TOUPPER "${CMAKE_BUILD_TYPE}" DISRUPTOR_BUILD_TYPE_UPPER)
set(DISRUPTOR_BENCH_BUILD_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${DISRUPTOR_BUILD_TYPE_UPPER}}")
string(STRIP "${DISRUPTOR_BENCH_BUILD_FLAGS}" DISRUPTOR_BENCH_BUILD_FLAGS)
add_executable(framework_benchmark src/benchmark.cpp)
target_include_directories(framework_benchmark PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_compile_definitions(framework_benchmark PRIVATE
    "DISRUPTOR_BUILD_TYPE=\"${CMAKE_BUILD_TYPE}\""
    "DISRUPTOR_BUILD_FLAGS=\"${DISRUPTOR_BENCH_BUILD_FLAGS}\"")

include(CTest)
if(BUILD_TESTING)
    add_executable(telemetry_correctness_test tests/telemetry_correctness_test.cpp)
    target_include_directories(telemetry_correctness_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    add_test(NAME telemetry_correctness COMMAND telemetry_correctness_test)

    add_executable(hybrid_wait_strategy_test tests/hybrid_wait_strategy_test.cpp)
    target_include_directories(hybrid_wait_strategy_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    add_test(NAME hybrid_wait_strategy COMMAND hybrid_wait_strategy_test)
endif()

set(
    DISRUPTOR_HEADERS
    src/disruptor/ring_buffer.h
    src/disruptor/sequence.h
    src/disruptor/sequencer.h
    src/disruptor/wait_strategies.h
    src/disruptor/sequence_barrier.h
    src/disruptor/event_handler.h
    src/disruptor/event_processor.h
)
target_sources(event_processing_examples PRIVATE ${DISRUPTOR_HEADERS})
