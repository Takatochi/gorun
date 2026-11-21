#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include "gorun/gorun.hpp"

using namespace std::chrono_literals;

#define GR_CHECK(cond)                                                                      \
    do {                                                                                    \
        if (!(cond)) {                                                                      \
            std::cerr << "[FAIL] " << #cond << " at " << __FILE__ << ":" << __LINE__ << "\n"; \
            std::exit(1);                                                                   \
        }                                                                                   \
    } while (0)

// ============================================================================
// Test 1: Configurable Stack Sizes & Multi-Bucket StackPool
// ============================================================================
static void test_configurable_stack_sizes() {
    gorun::Runtime rt(gorun::Config{.worker_count = 4, .default_stack_size = 128 * 1024, .max_pooled_stacks = 64});

    std::atomic<std::size_t> small_sz{0};
    std::atomic<std::size_t> default_sz{0};
    std::atomic<std::size_t> large_sz{0};
    std::atomic<std::size_t> custom_sz{0};
    std::atomic<std::uint64_t> large_checksum{0};

    gorun::WaitGroup wg;
    wg.add(4);

    rt.go(gorun::StackSize::Small, [&]() {
        small_sz.store(gorun::this_routine::stack_size());
        wg.done();
    });

    rt.go([&]() {
        default_sz.store(gorun::this_routine::stack_size());
        wg.done();
    });

    // Allocate a 256 KB buffer directly on the green routine stack — only possible with StackSize::Large!
    rt.go(gorun::StackSize::Large, [&]() {
        large_sz.store(gorun::this_routine::stack_size());
        volatile unsigned char big_frame[256 * 1024];
        std::uint64_t sum = 0;
        for (std::size_t i = 0; i < sizeof(big_frame); i += 4096) {
            big_frame[i] = static_cast<unsigned char>(i & 0xFF);
            sum += big_frame[i];
        }
        large_checksum.store(sum);
        wg.done();
    });

    // Custom 300 KB rounds up to 512 KB bucket cleanly
    rt.go(gorun::Options{.stack_size = 300 * 1024}, [&]() {
        custom_sz.store(gorun::this_routine::stack_size());
        wg.done();
    });

    wg.wait();
    GR_CHECK(small_sz.load() == 16 * 1024);
    GR_CHECK(default_sz.load() == 128 * 1024);
    GR_CHECK(large_sz.load() == 512 * 1024);
    GR_CHECK(custom_sz.load() == 512 * 1024);

    // Spawn 20 more Small routines and verify multi-bucket StackPool actually reuses stacks!
    wg.add(20);
    for (int i = 0; i < 20; ++i) {
        rt.go(gorun::StackSize::Small, [&]() {
            GR_CHECK(gorun::this_routine::stack_size() == 16 * 1024);
            wg.done();
        });
    }
    wg.wait();
    rt.wait_idle();

    auto st = rt.stats();
    GR_CHECK(st.stack_pool.total_reused > 0);
    std::cout << "[PASS] Test 1: Configurable StackSize (16KB/128KB/512KB) & multi-bucket StackPool reuse="
              << st.stack_pool.total_reused << "\n";
}

// ============================================================================
// Test 2: Native Async Network Poller (IOCP / epoll / kqueue) + Context Cancel
// ============================================================================
static void test_native_netpoller_tcp() {
    auto listener = gorun::net::TcpListener::bind("127.0.0.1", 0);
    const std::uint16_t port = listener.local_port();
    GR_CHECK(port != 0);

    auto [srv_ctx, srv_cancel] = gorun::with_cancel(gorun::background());
    gorun::WaitGroup srv_wg;
    srv_wg.add(1);

    // Accept loop running inside a green routine (zero OS worker blocking!)
    gorun::go([&]() {
        GORUN_DEFER { srv_wg.done(); };
        for (;;) {
            auto peer_opt = listener.accept(srv_ctx);
            if (!peer_opt) break;
            srv_wg.add(1);
            gorun::go(gorun::StackSize::Compact, [s = std::move(*peer_opt), &srv_wg]() mutable {
                GORUN_DEFER { srv_wg.done(); };
                char buf[1024];
                for (;;) {
                    auto r = s.read(buf, sizeof(buf));
                    if (!r.ok() || r.eof()) break;
                    auto w = s.write_all(std::string_view(buf, static_cast<std::size_t>(r.bytes)));
                    if (!w.ok()) break;
                }
            });
        }
    });

    // 1. 100 concurrent TCP echo clients on green routines
    constexpr int kClients = 100;
    std::atomic<int> echoed_ok{0};
    {
        gorun::Scope scope;
        for (int i = 0; i < kClients; ++i) {
            gorun::go(gorun::StackSize::Compact, [port, i, &echoed_ok]() {
                auto sock = gorun::net::TcpStream::connect("127.0.0.1", port);
                std::string payload = "hello-gorun-" + std::to_string(i);
                auto wr = sock.write_all(payload);
                if (!wr.ok()) return;
                char buf[128];
                auto rd = sock.read(buf, sizeof(buf), 2000ms);
                if (rd.ok() && std::string_view(buf, static_cast<std::size_t>(rd.bytes)) == payload) {
                    echoed_ok.fetch_add(1, std::memory_order_relaxed);
                }
            });
        }
    }
    GR_CHECK(echoed_ok.load() == kClients);

    // 2. Cooperative read timeout on an idle connection (never stalls worker!)
    {
        auto sock = gorun::net::TcpStream::connect("127.0.0.1", port);
        char buf[64];
        auto t0 = std::chrono::steady_clock::now();
        auto rd = sock.read(buf, sizeof(buf), 30ms);
        auto dt = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0);
        GR_CHECK(!rd.ok() && rd.ec == std::errc::timed_out);
        GR_CHECK(dt.count() < 1000);
    }

    // 3. Clean cancellation of accept() via gorun::Context
    srv_cancel();
    listener.close();
    srv_wg.wait();

    std::cout << "[PASS] Test 2: Native NetPoller TCP (" << kClients
              << "/100 concurrent echo clients + cooperative timeout & cancel)\n";
}

// ============================================================================
// Test 3: Elastic gorun::spawn_blocking + Non-Blocking File I/O (gorun::fs)
// ============================================================================
static void test_spawn_blocking_and_fs() {
    auto t0 = std::chrono::steady_clock::now();
    std::vector<gorun::JoinHandle<int>> handles;
    handles.reserve(12);
    for (int i = 0; i < 12; ++i) {
        handles.push_back(gorun::spawn_blocking([i]() {
            std::this_thread::sleep_for(25ms);
            return i * i;
        }));
    }

    int sum = 0;
    for (auto& h : handles) {
        sum += h.get();
    }
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0);
    // 12 x 25ms blocking sleeps executed concurrently on elastic pool (would take 300ms serially!)
    GR_CHECK(sum == 506); // sum(0^2..11^2)
    GR_CHECK(elapsed.count() < 220);

    // Exception propagation through spawn_blocking
    bool caught = false;
    try {
        gorun::spawn_blocking([]() -> int {
            throw std::runtime_error("blocking failure");
        }).get();
    } catch (const std::runtime_error& ex) {
        caught = (std::string_view(ex.what()) == "blocking failure");
    }
    GR_CHECK(caught);

    // Non-worker-blocking file write/append/read roundtrip via gorun::fs
    auto tmp_path = (std::filesystem::temp_directory_path() / "gorun_v04_fs_test.txt").string();
    gorun::default_runtime().block_on([&]() {
        gorun::fs::write_file(tmp_path, "hello ");
        gorun::fs::append_file(tmp_path, "gorun v0.4!");
        std::string content = gorun::fs::read_file(tmp_path);
        GR_CHECK(content == "hello gorun v0.4!");
    });
    std::error_code rm_ec;
    std::filesystem::remove(tmp_path, rm_ec);

    std::cout << "[PASS] Test 3: Elastic spawn_blocking (" << elapsed.count()
              << " ms for 12x25ms jobs) & gorun::fs async file I/O\n";
}

// ============================================================================
// Test 4: Bidirectional C++20 Coroutines (co_await) Bridge
// ============================================================================
static gorun::Task<int> coro_pipeline(gorun::Channel<int>& ch) {
    // 1. co_await a gorun green routine JoinHandle<int>
    int a = co_await gorun::go_async([]() {
        gorun::sleep_for(10ms);
        return 20;
    });

    // 2. co_await a blocking OS task (spawn_blocking)
    int b = co_await gorun::spawn_blocking([]() {
        std::this_thread::sleep_for(10ms);
        return 15;
    });

    // 3. co_await a gorun::Channel<int> receive
    auto c = co_await gorun::co_recv(ch);
    co_return a + b + c.value_or(0);
}

static gorun::Task<int> coro_throws() {
    co_await gorun::go_async([]() { gorun::sleep_for(5ms); });
    throw std::runtime_error("coro boom");
}

static void test_cpp20_coroutine_bridge() {
    gorun::Channel<int> ch(1);

    int total = gorun::default_runtime().block_on([&]() {
        gorun::go([&]() {
            gorun::sleep_for(15ms);
            ch.send(7);
        });
        // Await C++20 Task<int> directly inside a synchronous-looking gorun green routine!
        return gorun::await_coro(coro_pipeline(ch));
    });
    GR_CHECK(total == 42);

    // Verify exception propagation from C++20 coroutine back into green routine
    bool caught = false;
    gorun::default_runtime().block_on([&]() {
        try {
            (void)gorun::await_coro(coro_throws());
        } catch (const std::runtime_error& ex) {
            caught = (std::string_view(ex.what()) == "coro boom");
        }
    });
    GR_CHECK(caught);

    std::cout << "[PASS] Test 4: C++20 co_await bridge (Task<T>, JoinHandle, Channel, await_coro = "
              << total << ")\n";
}

// ============================================================================
// Test 5: Scheduler Observability (`Runtime::stats()`)
// ============================================================================
static void test_runtime_stats_observability() {
    gorun::Runtime rt(gorun::Config{.worker_count = 4});
    gorun::Channel<int> gate(0);
    gorun::WaitGroup parked_ready;
    parked_ready.add(10);

    for (int i = 0; i < 10; ++i) {
        rt.go([&]() {
            parked_ready.done();
            (void)gate.recv();
        });
    }
    parked_ready.wait();
    std::this_thread::sleep_for(20ms);

    auto mid_stats = rt.stats();
    GR_CHECK(mid_stats.worker_count == 4);
    GR_CHECK(mid_stats.active_routines == 10);
    GR_CHECK(mid_stats.parked_routines == 10);
    GR_CHECK(mid_stats.total_spawned == 10);
    GR_CHECK(mid_stats.local_queue_sizes.size() == 4);

    gate.close();
    rt.wait_idle();

    auto end_stats = rt.stats();
    GR_CHECK(end_stats.active_routines == 0);
    GR_CHECK(end_stats.parked_routines == 0);
    GR_CHECK(end_stats.total_completed == 10);

    std::cout << "[PASS] Test 5: Runtime::stats() observability (spawned=" << end_stats.total_spawned
              << ", completed=" << end_stats.total_completed
              << ", steals=" << end_stats.total_steals
              << ", arch=" << gorun::detail::kArchitectureName << ")\n";
}

int main() {
    std::cout << "=== gorun v0.4.0 Foundation & Runtime Expansion Tests ("
              << gorun::detail::kArchitectureName << ") ===\n";
    test_configurable_stack_sizes();
    test_native_netpoller_tcp();
    test_spawn_blocking_and_fs();
    test_cpp20_coroutine_bridge();
    test_runtime_stats_observability();
    std::cout << "=== All gorun v0.4.0 tests passed! ===\n";
    return 0;
}
