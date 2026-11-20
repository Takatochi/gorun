/**
 * @file bench_gorun.cpp
 * @brief Performance Benchmark Suite for `gorun` (C++20 M:N Green Threads Runtime).
 *
 * Designed for direct 1-to-1 comparison against `benchmarks/bench_go.go` (Go native runtime)
 * and standard C++ OS threads (`std::jthread` / `std::mutex`).
 *
 * Benchmarks:
 *  1. Unbuffered Rendezvous Channel Ping-Pong (capacity = 0, stack-to-stack latency & ops/sec)
 *  2. Buffered Channel Throughput (capacity = 256, 1,000,000 messages)
 *  3. Massive Spawn & Join Throughput (50,000 short-lived green routines via StackPool)
 *  4. High-Contention Mutex Benchmark (100 green routines x 2,000 lock/unlock cycles = 200,000 ops)
 *  5. Pure Context Switch / Yield Latency (100,000 cooperative yields on 1 worker)
 */

#include <atomic>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <string>
#include <vector>

#include "gorun/gorun.hpp"

using Clock = std::chrono::steady_clock;

struct BenchResult {
    std::string name;
    std::uint64_t operations;
    double elapsed_ms;
    double ns_per_op;
    double ops_per_sec;
};

static void print_result(const BenchResult& r) {
    std::cout << std::left << std::setw(46) << r.name
              << " | " << std::right << std::setw(10) << r.operations << " ops"
              << " | " << std::setw(8) << std::fixed << std::setprecision(2) << r.elapsed_ms << " ms"
              << " | " << std::setw(8) << std::fixed << std::setprecision(1) << r.ns_per_op << " ns/op"
              << " | " << std::setw(10) << std::fixed << std::setprecision(2) << (r.ops_per_sec / 1e6) << " M ops/s\n";
}

// 1. Unbuffered Rendezvous Channel Ping-Pong (100,000 messages)
BenchResult bench_unbuffered_ping_pong() {
    constexpr std::uint64_t kMessages = 100'000;
    gorun::Channel<int> ch(0);
    gorun::WaitGroup wg;
    wg.add(2);

    auto t0 = Clock::now();

    gorun::go([&]() {
        for (std::uint64_t i = 0; i < kMessages; ++i) {
            ch.send(1);
        }
        ch.close();
        wg.done();
    });

    gorun::go([&]() {
        std::uint64_t sum = 0;
        while (auto v = ch.recv()) {
            sum += *v;
        }
        if (sum != kMessages) std::abort();
        wg.done();
    });

    wg.wait();
    auto t1 = Clock::now();

    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    double ns = std::chrono::duration<double, std::nano>(t1 - t0).count() / static_cast<double>(kMessages);
    return BenchResult{"1. Unbuffered Channel Rendezvous (cap=0)", kMessages, ms, ns, (kMessages * 1000.0) / ms};
}

// 2. Buffered Channel High-Throughput (1,000,000 messages, cap=256)
BenchResult bench_buffered_channel_throughput() {
    constexpr std::uint64_t kMessages = 1'000'000;
    gorun::Channel<int> ch(256);
    gorun::WaitGroup wg;
    wg.add(2);

    auto t0 = Clock::now();

    gorun::go([&]() {
        for (std::uint64_t i = 0; i < kMessages; ++i) {
            ch.send(1);
        }
        ch.close();
        wg.done();
    });

    gorun::go([&]() {
        std::uint64_t sum = 0;
        while (auto v = ch.recv()) {
            sum += *v;
        }
        if (sum != kMessages) std::abort();
        wg.done();
    });

    wg.wait();
    auto t1 = Clock::now();

    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    double ns = std::chrono::duration<double, std::nano>(t1 - t0).count() / static_cast<double>(kMessages);
    return BenchResult{"2. Buffered Channel Throughput (cap=256)", kMessages, ms, ns, (kMessages * 1000.0) / ms};
}

// 3. Spawn & Wait 50,000 Green Routines
BenchResult bench_massive_spawn() {
    constexpr std::uint64_t kRoutines = 50'000;
    std::atomic<std::uint64_t> counter{0};

    auto t0 = Clock::now();
    {
        gorun::Scope scope;
        for (std::uint64_t i = 0; i < kRoutines; ++i) {
            gorun::go([&]() {
                counter.fetch_add(1, std::memory_order_relaxed);
            });
        }
    }
    auto t1 = Clock::now();

    if (counter.load() != kRoutines) std::abort();
    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    double ns = std::chrono::duration<double, std::nano>(t1 - t0).count() / static_cast<double>(kRoutines);
    return BenchResult{"3. Spawn & Join 50,000 Green Routines", kRoutines, ms, ns, (kRoutines * 1000.0) / ms};
}

// 4. High-Contention Green Mutex (100 routines x 2,000 locks = 200,000 ops)
BenchResult bench_contended_mutex() {
    constexpr std::uint64_t kRoutines = 100;
    constexpr std::uint64_t kLocksPerRoutine = 2'000;
    constexpr std::uint64_t kTotalOps = kRoutines * kLocksPerRoutine;

    gorun::Mutex mtx;
    std::uint64_t shared_val = 0;

    auto t0 = Clock::now();
    {
        gorun::Scope scope;
        for (std::uint64_t r = 0; r < kRoutines; ++r) {
            gorun::go([&]() {
                for (std::uint64_t i = 0; i < kLocksPerRoutine; ++i) {
                    std::lock_guard<gorun::Mutex> lk(mtx);
                    ++shared_val;
                }
            });
        }
    }
    auto t1 = Clock::now();

    if (shared_val != kTotalOps) std::abort();
    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    double ns = std::chrono::duration<double, std::nano>(t1 - t0).count() / static_cast<double>(kTotalOps);
    return BenchResult{"4. Contended gorun::Mutex (100 routines)", kTotalOps, ms, ns, (kTotalOps * 1000.0) / ms};
}

// 5. Pure Context Switch / Cooperative Yield Latency (100,000 yields on 1-worker runtime)
BenchResult bench_context_switch_yield() {
    constexpr std::uint64_t kYields = 100'000;
    gorun::Runtime single_rt(gorun::Config{.worker_count = 1});

    auto t0 = Clock::now();
    single_rt.block_on([&]() {
        for (std::uint64_t i = 0; i < kYields; ++i) {
            gorun::yield();
        }
    });
    auto t1 = Clock::now();

    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    double ns = std::chrono::duration<double, std::nano>(t1 - t0).count() / static_cast<double>(kYields);
    return BenchResult{"5. User-Space Context Switch (gorun::yield)", kYields, ms, ns, (kYields * 1000.0) / ms};
}

int main() {
    std::cout << "==========================================================================================================\n";
    std::cout << "  C++20 gorun M:N Green Threads Benchmark Suite (Workers: " << gorun::default_runtime().worker_count() << ")\n";
    std::cout << "==========================================================================================================\n";

    // Warmup pool
    {
        gorun::Scope warmup;
        for (int i = 0; i < 256; ++i) {
            gorun::go([]() {});
        }
    }

    print_result(bench_unbuffered_ping_pong());
    print_result(bench_buffered_channel_throughput());
    print_result(bench_massive_spawn());
    print_result(bench_contended_mutex());
    print_result(bench_context_switch_yield());

    std::cout << "==========================================================================================================\n";
    return 0;
}
