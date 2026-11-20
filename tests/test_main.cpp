/**
 * @file test_main.cpp
 * @brief Comprehensive 20-suite unit, integration, race-condition, and memory-leak test suite for `gorun`.
 *
 * Covers:
 *  1. Ergonomic `go(...)` + buffered `Channel<T>` directly in `main()`.
 *  2. Unbuffered Rendezvous channel (`capacity == 0`) with Direct Stack-to-Stack Handoff.
 *  3. Move-only payload types (`std::unique_ptr<T>`) across channels.
 *  4. Stack Stability invariant (local stack variable addresses never relocate across 100 `park`/`yield` cycles).
 *  5. Deep recursion on an isolated 128 KB routine stack with a hardware Guard Page.
 *  6. Structured concurrency (`TaskGroup` / `Scope`) and transitive scope inheritance in nested `go(...)` calls.
 *  7. Exception Safety and automatic cooperative cancellation (`std::stop_token`) of sibling routines on failure.
 *  8. `gorun::Mutex` + `std::lock_guard` / `std::unique_lock` — Race Condition stress test (50,000 increments).
 *  9. Zero OS-thread blocking proof: routines sleeping on `Mutex`, `Channel`, and `sleep_for` on a 1-Worker `Runtime`.
 * 10. `gorun::CondVar` (`notify_one` and `notify_all` broadcast with predicate).
 * 11. `gorun::WaitGroup` (concurrent waiting across multiple watcher routines).
 * 12. `gorun::Semaphore` (strict concurrency bound verification under 60-routine contention).
 * 13. `gorun::Once` / `gorun::call_once` (exact single execution under 100-routine contention).
 * 14. `gorun::RwMutex` (concurrent readers `std::shared_lock` + exclusive writers `std::unique_lock` consistency).
 * 15. `gorun::select(...)` (multi-channel multiplexing with `on_recv`, `on_send`, `on_default`, and `on_timeout`).
 * 16. `gorun::go_async(...)` and `JoinHandle<T>` (value retrieval and exception propagation).
 * 17. `gorun::RoutineLocal<T>` (per-routine state isolation across OS worker thread migrations).
 * 18. Extreme M:N Work-Stealing & Lost-Wakeup Race Condition Stress Test (1,000 routines, 3-stage MPMC pipeline).
 * 19. `gorun::defer(...)` and `GORUN_DEFER { ... };` (LIFO execution order, `.dismiss()`, and exception-safe cleanup).
 * 20. Zero Lost Pointers & Exact Constructor/Destructor Lifetime Audit (10,500 heap objects across closures & channels).
 */

#include <atomic>
#include <cassert>
#include <chrono>
#include <iostream>
#include <memory>
#include <mutex>
#include <numeric>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <vector>

#include "gorun/gorun.hpp"

using namespace std::chrono_literals;

#define GORUN_TEST_ASSERT(cond)                                                                      \
    do {                                                                                             \
        if (!(cond)) {                                                                               \
            std::cerr << "\n[ASSERTION FAILED] " << #cond << " at " << __FILE__ << ":" << __LINE__   \
                      << std::endl;                                                                  \
            std::abort();                                                                            \
        }                                                                                            \
    } while (0)

// 1. Ergonomic go(...) launch and buffered channel directly in main()
void test_01_ergonomic_go_and_buffered_channel() {
    std::cout << "[TEST 01] Ergonomic go(...) + Buffered Channel in main()... ";
    gorun::Channel<int> ch(8);

    gorun::go([&]() {
        for (int i = 1; i <= 8; ++i) {
            GORUN_TEST_ASSERT(ch.send(i * 10));
        }
        ch.close();
    });

    int sum = 0;
    int count = 0;
    while (auto v = ch.recv()) {
        sum += *v;
        ++count;
    }

    GORUN_TEST_ASSERT(count == 8);
    GORUN_TEST_ASSERT(sum == 360);
    GORUN_TEST_ASSERT(ch.is_closed());
    std::cout << "PASSED\n";
}

// 2. Unbuffered Rendezvous channel (capacity == 0) — Direct Stack-to-Stack Handoff
void test_02_unbuffered_rendezvous_channel() {
    std::cout << "[TEST 02] Unbuffered Rendezvous Channel (capacity == 0, Stack-to-Stack)... ";
    gorun::Channel<std::string> ch(0);
    gorun::WaitGroup wg;
    wg.add(2);

    std::string received;

    gorun::go([&]() {
        bool ok = ch.send("Direct stack-to-stack message");
        GORUN_TEST_ASSERT(ok);
        wg.done();
    });

    gorun::go([&]() {
        gorun::sleep_for(5ms);
        auto msg = ch.recv();
        GORUN_TEST_ASSERT(msg.has_value());
        received = *msg;
        wg.done();
    });

    wg.wait();
    GORUN_TEST_ASSERT(received == "Direct stack-to-stack message");
    std::cout << "PASSED\n";
}

// 3. Move-only types (std::unique_ptr<T>) in buffered and unbuffered channels
void test_03_move_only_types_in_channel() {
    std::cout << "[TEST 03] Move-only types (std::unique_ptr<T>) in Channel... ";
    gorun::Channel<std::unique_ptr<int>> ch(0);

    gorun::go([&]() {
        for (int i = 1; i <= 10; ++i) {
            ch.send(std::make_unique<int>(i * 5));
        }
        ch.close();
    });

    int total = 0;
    while (auto ptr = ch.recv()) {
        GORUN_TEST_ASSERT(*ptr != nullptr);
        total += **ptr;
    }

    GORUN_TEST_ASSERT(total == 275);
    std::cout << "PASSED\n";
}

// 4. Stack Stability invariant: local stack variable addresses never change across park/yield
void test_04_stack_stability_invariant() {
    std::cout << "[TEST 04] Stack Stability invariant across 100 context switches... ";
    {
        gorun::Scope scope;
        for (int i = 0; i < 64; ++i) {
            gorun::go([i]() {
                alignas(64) int stack_array[16];
                for (int k = 0; k < 16; ++k) stack_array[k] = i + k;
                const int* original_ptr = &stack_array[0];

                for (int step = 0; step < 100; ++step) {
                    gorun::yield();
                    GORUN_TEST_ASSERT(&stack_array[0] == original_ptr);
                    stack_array[0] += 1;
                }
                GORUN_TEST_ASSERT(stack_array[0] == i + 100);
            });
        }
    }
    std::cout << "PASSED\n";
}

// 5. Deep recursion on an isolated 128 KB routine stack
static int recursive_fibonacci_sum(int depth, volatile int* anchor) {
    volatile char buffer[256]{};
    buffer[0] = static_cast<char>(depth);
    if (depth <= 0) {
        return static_cast<int>(buffer[0]) + (*anchor);
    }
    if (depth % 25 == 0) {
        gorun::yield();
    }
    return 1 + recursive_fibonacci_sum(depth - 1, anchor);
}

void test_05_deep_stack_recursion() {
    std::cout << "[TEST 05] Deep Recursion on isolated 128 KB routine stack... ";
    int result = 0;
    {
        gorun::Scope scope;
        gorun::go([&]() {
            volatile int anchor = 10;
            result = recursive_fibonacci_sum(120, &anchor);
        });
    }
    GORUN_TEST_ASSERT(result == 130);
    std::cout << "PASSED\n";
}

// 6. Structured concurrency: nested go([&]) calls automatically attach to the active parent Scope
void test_06_nested_structured_scope() {
    std::cout << "[TEST 06] Transitive Structured Concurrency (nested go([&]) inside Scope)... ";
    std::atomic<int> completed_leaves{0};
    std::vector<int> local_slots(16, 0);

    {
        gorun::Scope scope;
        for (int i = 0; i < 4; ++i) {
            // Level 1: plain go([&]) call
            gorun::go([&, i]() {
                for (int j = 0; j < 4; ++j) {
                    int idx = i * 4 + j;
                    // Level 2: nested routine also calls go([&]) without explicitly passing `scope`!
                    gorun::go([&, idx]() {
                        gorun::sleep_for(5ms);
                        local_slots[idx] = idx + 1;
                        completed_leaves.fetch_add(1, std::memory_order_relaxed);
                    });
                }
            });
        }
    } // <-- ~Scope() destructor waits for both Level 1 and all Level 2 nested routines!

    GORUN_TEST_ASSERT(completed_leaves.load() == 16);
    for (int i = 0; i < 16; ++i) {
        GORUN_TEST_ASSERT(local_slots[i] == i + 1);
    }
    std::cout << "PASSED\n";
}

// 7. Exception Safety and automatic cooperative cancellation via std::stop_token in TaskGroup
void test_07_task_group_exception_and_cancellation() {
    std::cout << "[TEST 07] TaskGroup Exception propagation & automatic std::stop_token cancel... ";
    std::atomic<int> stopped_count{0};
    bool exception_caught = false;

    try {
        gorun::TaskGroup group;

        for (int i = 0; i < 8; ++i) {
            group.spawn([&](std::stop_token st) {
                while (!st.stop_requested()) {
                    gorun::yield();
                }
                stopped_count.fetch_add(1, std::memory_order_relaxed);
            });
        }

        group.spawn([]() {
            gorun::sleep_for(10ms);
            throw std::invalid_argument("Critical error in child routine");
        });

        group.wait();
    } catch (const std::invalid_argument& ex) {
        exception_caught = true;
        GORUN_TEST_ASSERT(std::string(ex.what()) == "Critical error in child routine");
    }

    GORUN_TEST_ASSERT(exception_caught);
    GORUN_TEST_ASSERT(stopped_count.load() == 8);
    std::cout << "PASSED\n";
}

// 8. Race Condition Stress Test for gorun::Mutex (250 routines x 200 critical sections = 50,000 operations)
void test_08_mutex_race_condition_stress() {
    std::cout << "[TEST 08] gorun::Mutex Race Condition Stress Test (50,000 critical sections)... ";
    gorun::Mutex mtx;
    std::int64_t non_atomic_counter = 0;
    std::int64_t invariant_a = 0;
    std::int64_t invariant_b = 0;

    {
        gorun::Scope scope;
        for (int r = 0; r < 250; ++r) {
            gorun::go([&]() {
                for (int step = 0; step < 200; ++step) {
                    std::lock_guard<gorun::Mutex> lock(mtx);
                    // Verify that the two-variable invariant is never observed broken inside the critical section
                    GORUN_TEST_ASSERT(invariant_a + invariant_b == 0);
                    ++invariant_a;
                    ++non_atomic_counter;
                    --invariant_b;
                    GORUN_TEST_ASSERT(invariant_a + invariant_b == 0);
                }
            });
        }
    }

    GORUN_TEST_ASSERT(non_atomic_counter == 50000);
    std::cout << "PASSED\n";
}

// 9. Zero OS-thread blocking invariant proof:
// Creates an isolated Runtime with ONLY 1 OS WORKER THREAD (worker_count = 1).
// Spawns routines that sleep on Mutex, Channel, and sleep_for, proving the single OS worker thread never blocks!
void test_09_zero_os_thread_blocking_single_worker() {
    std::cout << "[TEST 09] Zero OS-thread blocking invariant on 1-Worker Runtime... ";
    gorun::Runtime single_thread_rt(gorun::Config{.worker_count = 1});

    int scoped_result = single_thread_rt.block_on([&]() -> int {
        gorun::Mutex mtx;
        gorun::Channel<int> ch(0);
        gorun::Channel<bool> r1_locked_ch(1);
        int val = 0;
        {
            gorun::Scope scope(single_thread_rt);

            // Routine 1 acquires mtx, signals r1_locked_ch, and parks on unbuffered channel ch.
            // If Mutex or Channel blocked the single OS worker thread (worker_count = 1), this would deadlock forever!
            scope.spawn([&]() {
                std::lock_guard lock(mtx);
                r1_locked_ch.send(true);
                auto v = ch.recv();
                GORUN_TEST_ASSERT(v.has_value());
                val += *v;
            });

            // Routine 2 waits until Routine 1 holds mtx, then contends on mtx (parks on Mutex)
            scope.spawn([&]() {
                (void)r1_locked_ch.recv();
                std::lock_guard lock(mtx);
                val *= 10;
            });

            // Routine 3 sleeps 10 ms on the timer thread and unparks Routine 1 via ch
            scope.spawn([&]() {
                gorun::sleep_for(10ms);
                ch.send(42);
            });
        }
        return val;
    });

    GORUN_TEST_ASSERT(scoped_result == 420);
    std::cout << "PASSED\n";
}

// 10. Test gorun::CondVar (notify_one and notify_all broadcast)
void test_10_condvar_broadcast_and_signal() {
    std::cout << "[TEST 10] gorun::CondVar (notify_one & notify_all with predicate)... ";
    gorun::Mutex mtx;
    gorun::CondVar cv;
    int stage = 0;
    std::atomic<int> awakened{0};

    {
        gorun::Scope scope;
        for (int i = 0; i < 16; ++i) {
            gorun::go([&]() {
                std::unique_lock<gorun::Mutex> lk(mtx);
                cv.wait(lk, [&]() { return stage == 1; });
                awakened.fetch_add(1, std::memory_order_relaxed);
            });
        }

        gorun::go([&]() {
            gorun::sleep_for(10ms);
            {
                std::lock_guard<gorun::Mutex> lk(mtx);
                stage = 1;
            }
            cv.notify_all();
        });
    }

    GORUN_TEST_ASSERT(awakened.load() == 16);
    std::cout << "PASSED\n";
}

// 11. Test gorun::WaitGroup with multiple concurrent waiting routines
void test_11_waitgroup_multi_waiters() {
    std::cout << "[TEST 11] gorun::WaitGroup with multiple concurrent waiting routines... ";
    gorun::WaitGroup work_wg;
    gorun::WaitGroup watchers_wg;
    std::atomic<int> work_done{0};
    std::atomic<int> watchers_released{0};

    work_wg.add(10);
    watchers_wg.add(5);

    // 5 watcher routines concurrently call work_wg.wait()
    for (int w = 0; w < 5; ++w) {
        gorun::go([&]() {
            work_wg.wait();
            GORUN_TEST_ASSERT(work_done.load(std::memory_order_acquire) == 10);
            watchers_released.fetch_add(1, std::memory_order_relaxed);
            watchers_wg.done();
        });
    }

    // 10 worker routines complete their tasks
    for (int i = 0; i < 10; ++i) {
        gorun::go([&]() {
            gorun::yield();
            work_done.fetch_add(1, std::memory_order_release);
            work_wg.done();
        });
    }

    watchers_wg.wait();
    GORUN_TEST_ASSERT(watchers_released.load() == 5);
    std::cout << "PASSED\n";
}

// 12. Test gorun::Semaphore (strict concurrent access limit)
void test_12_semaphore_concurrency_limit() {
    std::cout << "[TEST 12] gorun::Semaphore strict concurrency bound... ";
    constexpr int kMaxPermits = 3;
    gorun::Semaphore sem(kMaxPermits);
    std::atomic<int> active{0};
    std::atomic<int> peak_active{0};
    std::atomic<int> total_completed{0};

    {
        gorun::Scope scope;
        for (int i = 0; i < 60; ++i) {
            gorun::go([&]() {
                sem.acquire();
                int cur = active.fetch_add(1, std::memory_order_seq_cst) + 1;
                GORUN_TEST_ASSERT(cur <= kMaxPermits);

                int prev = peak_active.load();
                while (cur > prev && !peak_active.compare_exchange_weak(prev, cur)) {}

                gorun::yield();
                active.fetch_sub(1, std::memory_order_seq_cst);
                total_completed.fetch_add(1, std::memory_order_relaxed);
                sem.release();
            });
        }
    }

    GORUN_TEST_ASSERT(peak_active.load() <= kMaxPermits);
    GORUN_TEST_ASSERT(total_completed.load() == 60);
    std::cout << "PASSED\n";
}

// 13. Test gorun::Once / gorun::call_once under high contention
void test_13_call_once_race() {
    std::cout << "[TEST 13] gorun::Once / call_once under 100-routine contention... ";
    gorun::Once once;
    std::atomic<int> counter{0};
    std::string shared_resource;

    {
        gorun::Scope scope;
        for (int i = 0; i < 100; ++i) {
            gorun::go([&]() {
                gorun::call_once(once, [&]() {
                    gorun::sleep_for(5ms);
                    counter.fetch_add(1, std::memory_order_relaxed);
                    shared_resource = "Initialized exactly once";
                });
                // Every routine returning from call_once() is guaranteed to observe the initialized resource!
                GORUN_TEST_ASSERT(shared_resource == "Initialized exactly once");
            });
        }
    }

    GORUN_TEST_ASSERT(counter.load() == 1);
    std::cout << "PASSED\n";
}

// 14. Stress test gorun::RwMutex (readers never observe a partially written state from a writer)
void test_14_rwmutex_readers_writers_consistency() {
    std::cout << "[TEST 14] gorun::RwMutex Readers/Writers invariant check... ";
    gorun::RwMutex rw;
    std::array<int, 8> data{0, 0, 0, 0, 0, 0, 0, 0};

    {
        gorun::Scope scope;
        // 10 writers atomically populate all 8 elements with the same value
        for (int w = 1; w <= 10; ++w) {
            gorun::go([&, w]() {
                for (int step = 0; step < 20; ++step) {
                    std::unique_lock<gorun::RwMutex> write_lock(rw);
                    for (int idx = 0; idx < 8; ++idx) {
                        data[idx] = w * 100 + step;
                    }
                }
            });
        }

        // 40 readers concurrently verify that all 8 array elements are always identical
        for (int r = 0; r < 40; ++r) {
            gorun::go([&]() {
                for (int step = 0; step < 50; ++step) {
                    std::shared_lock<gorun::RwMutex> read_lock(rw);
                    int first = data[0];
                    for (int idx = 1; idx < 8; ++idx) {
                        GORUN_TEST_ASSERT(data[idx] == first);
                    }
                    gorun::yield();
                }
            });
        }
    }
    std::cout << "PASSED\n";
}

// 15. Test multi-channel multiplexer gorun::select(...) (on_recv, on_send, on_default, on_timeout)
void test_15_channel_select_multiplexing() {
    std::cout << "[TEST 15] gorun::select(...) (on_recv, on_send, on_default, on_timeout)... ";
    gorun::Channel<int> ch1(0);
    gorun::Channel<int> ch2(1);

    // 15.1. Verify on_default when channels are empty
    bool default_taken = false;
    gorun::select(
        ch1.on_recv([&](int) { GORUN_TEST_ASSERT(false); }),
        gorun::on_default([&]() { default_taken = true; })
    );
    GORUN_TEST_ASSERT(default_taken);

    // 15.2. Verify on_send into buffered channel ch2
    bool sent_to_ch2 = false;
    gorun::select(
        ch2.on_send(555, [&]() { sent_to_ch2 = true; }),
        gorun::on_default([&]() { GORUN_TEST_ASSERT(false); })
    );
    GORUN_TEST_ASSERT(sent_to_ch2);

    // 15.3. Verify on_recv from ch2
    int received_val = 0;
    gorun::select(
        ch1.on_recv([&](int) { GORUN_TEST_ASSERT(false); }),
        ch2.on_recv([&](int v) { received_val = v; })
    );
    GORUN_TEST_ASSERT(received_val == 555);

    // 15.4. Verify on_timeout
    bool timed_out = false;
    gorun::select(
        ch1.on_recv([&](int) { GORUN_TEST_ASSERT(false); }),
        gorun::on_timeout(15ms, [&]() { timed_out = true; })
    );
    GORUN_TEST_ASSERT(timed_out);
    std::cout << "PASSED\n";
}

// 16. Test gorun::go_async(...) and JoinHandle<T> (return value and exception propagation)
void test_16_go_async_join_handle() {
    std::cout << "[TEST 16] gorun::go_async(...) & JoinHandle<T> value and exception... ";

    auto h1 = gorun::go_async([]() -> std::string {
        gorun::sleep_for(5ms);
        return "Async result 42";
    });

    auto h2 = gorun::go_async([]() -> int {
        gorun::yield();
        throw std::domain_error("Async computation failed");
    });

    GORUN_TEST_ASSERT(h1.get() == "Async result 42");

    bool caught = false;
    try {
        (void)h2.get();
    } catch (const std::domain_error& e) {
        caught = true;
        GORUN_TEST_ASSERT(std::string(e.what()) == "Async computation failed");
    }
    GORUN_TEST_ASSERT(caught);
    std::cout << "PASSED\n";
}

// 17. Test gorun::RoutineLocal<T> (safe replacement for thread_local across worker migrations)
void test_17_routine_local_storage() {
    std::cout << "[TEST 17] gorun::RoutineLocal<T> isolation across worker migrations... ";
    gorun::RoutineLocal<int> rlocal;

    {
        gorun::Scope scope;
        for (int i = 1; i <= 50; ++i) {
            gorun::go([&, i]() {
                *rlocal = i * 1000;
                for (int step = 1; step <= 20; ++step) {
                    gorun::yield();
                    *rlocal += 1;
                }
                GORUN_TEST_ASSERT(*rlocal == i * 1000 + 20);
            });
        }
    }
    std::cout << "PASSED\n";
}

// 18. Extreme M:N Work-Stealing & Lost-Wakeup Race Condition Stress Test
// 1,000 routines + MPMC pipeline with small buffered and unbuffered rendezvous channels
void test_18_lost_wakeup_and_work_stealing_stress() {
    std::cout << "[TEST 18] Extreme Park/Unpark & Work-Stealing Stress Test (1,000 routines)... ";

    // Small capacity (2) + unbuffered (0) channels force thousands of concurrent park_locked() / unpark() transitions
    gorun::Channel<int> stage1_ch(2);
    gorun::Channel<int> stage2_ch(0);

    constexpr int kProducers = 25;
    constexpr int kItemsPerProducer = 200; // 5,000 messages across a 2-stage pipeline
    constexpr int kTransformers = 25;
    constexpr int kConsumers = 25;

    gorun::WaitGroup prod_wg;
    prod_wg.add(kProducers);

    gorun::WaitGroup trans_wg;
    trans_wg.add(kTransformers);

    gorun::WaitGroup cons_wg;
    cons_wg.add(kConsumers);

    std::atomic<long long> total_received{0};
    std::atomic<long long> total_sum{0};

    // Stage 1: Producers push into stage1_ch
    for (int p = 0; p < kProducers; ++p) {
        gorun::go([&]() {
            for (int i = 1; i <= kItemsPerProducer; ++i) {
                stage1_ch.send(2);
            }
            prod_wg.done();
        });
    }

    // Stage 2: Transformers read from stage1_ch, multiply by 3, and forward into unbuffered stage2_ch
    for (int t = 0; t < kTransformers; ++t) {
        gorun::go([&]() {
            while (auto v = stage1_ch.recv()) {
                stage2_ch.send((*v) * 3);
            }
            trans_wg.done();
        });
    }

    // Stage 3: Consumers read from unbuffered stage2_ch
    for (int c = 0; c < kConsumers; ++c) {
        gorun::go([&]() {
            long long local_count = 0;
            long long local_sum = 0;
            while (auto v = stage2_ch.recv()) {
                ++local_count;
                local_sum += *v;
            }
            total_received.fetch_add(local_count, std::memory_order_relaxed);
            total_sum.fetch_add(local_sum, std::memory_order_relaxed);
            cons_wg.done();
        });
    }

    // Pipeline close coordinators
    gorun::go([&]() {
        prod_wg.wait();
        stage1_ch.close();
    });

    gorun::go([&]() {
        trans_wg.wait();
        stage2_ch.close();
    });

    cons_wg.wait();

    GORUN_TEST_ASSERT(total_received.load() == kProducers * kItemsPerProducer);
    GORUN_TEST_ASSERT(total_sum.load() == static_cast<long long>(kProducers) * kItemsPerProducer * 6);
    std::cout << "PASSED\n";
}

// 19. Test gorun::defer(...) and GORUN_DEFER { ... }; macro
// Verifies:
// - LIFO execution order of multiple defers in the same scope (matching Go's defer stack)
// - Automatic invocation of wg.done() and ch.close() on early return or exception unwinding
// - Disarming a deferred action via guard.dismiss()
void test_19_defer_lifo_and_exception_cleanup() {
    std::cout << "[TEST 19] gorun::defer(...) & GORUN_DEFER (LIFO order, dismiss, exception safety)... ";

    // 19.1. LIFO execution order of multiple defers (last registered runs first, like Go)
    std::vector<int> execution_order;
    {
        auto d1 = gorun::defer([&]() { execution_order.push_back(1); });
        GORUN_DEFER { execution_order.push_back(2); };
        GORUN_DEFER { execution_order.push_back(3); };

        auto cancelled = gorun::defer([&]() { execution_order.push_back(999); });
        GORUN_TEST_ASSERT(cancelled.is_active());
        cancelled.dismiss();
        GORUN_TEST_ASSERT(!cancelled.is_active());
    }
    GORUN_TEST_ASSERT(execution_order.size() == 3);
    GORUN_TEST_ASSERT(execution_order[0] == 3);
    GORUN_TEST_ASSERT(execution_order[1] == 2);
    GORUN_TEST_ASSERT(execution_order[2] == 1);

    // 19.2. Classic Go pattern: `defer wg.Done()` and `defer close(ch)` even when an exception is thrown
    gorun::Channel<int> ch(4);
    gorun::WaitGroup wg;
    wg.add(1);

    gorun::go([&]() {
        GORUN_DEFER { wg.done(); };
        GORUN_DEFER { ch.close(); };

        ch.send(100);
        ch.send(200);
        // Throw mid-routine — both GORUN_DEFER guards must still close the channel and signal wg.done()!
        throw std::runtime_error("Early abort inside green routine");
    });

    wg.wait();
    GORUN_TEST_ASSERT(ch.is_closed());
    auto v1 = ch.recv();
    auto v2 = ch.recv();
    auto v3 = ch.recv();
    GORUN_TEST_ASSERT(v1.has_value() && *v1 == 100);
    GORUN_TEST_ASSERT(v2.has_value() && *v2 == 200);
    GORUN_TEST_ASSERT(!v3.has_value());

    std::cout << "PASSED\n";
}

// 20. Mathematical verification of zero lost pointers (Zero Pointer Leak & Lifetime Audit)
// Tracks every constructor, copy/move, and destructor for TrackedObject instances across
// routine closures (`r->task`), channels, and `run_next_` steals.
struct TrackedObject {
    static inline std::atomic<std::int64_t> live_instances{0};
    static inline std::atomic<std::int64_t> total_constructed{0};
    static inline std::atomic<std::int64_t> total_destroyed{0};

    int payload{0};

    explicit TrackedObject(int p = 0) noexcept : payload(p) {
        live_instances.fetch_add(1, std::memory_order_relaxed);
        total_constructed.fetch_add(1, std::memory_order_relaxed);
    }

    TrackedObject(const TrackedObject& other) noexcept : payload(other.payload) {
        live_instances.fetch_add(1, std::memory_order_relaxed);
        total_constructed.fetch_add(1, std::memory_order_relaxed);
    }

    TrackedObject(TrackedObject&& other) noexcept : payload(std::exchange(other.payload, -1)) {
        live_instances.fetch_add(1, std::memory_order_relaxed);
        total_constructed.fetch_add(1, std::memory_order_relaxed);
    }

    TrackedObject& operator=(const TrackedObject& other) noexcept {
        payload = other.payload;
        return *this;
    }

    TrackedObject& operator=(TrackedObject&& other) noexcept {
        payload = std::exchange(other.payload, -1);
        return *this;
    }

    ~TrackedObject() noexcept {
        live_instances.fetch_sub(1, std::memory_order_relaxed);
        total_destroyed.fetch_add(1, std::memory_order_relaxed);
    }
};

void test_20_zero_lost_pointers_and_lifetime_audit() {
    std::cout << "[TEST 20] Zero Lost Pointers & Exact Constructor/Destructor Audit (10,000 objects)... ";

    TrackedObject::live_instances.store(0);
    TrackedObject::total_constructed.store(0);
    TrackedObject::total_destroyed.store(0);

    std::atomic<std::int64_t> consumed_payload_sum{0};

    {
        // Isolated Runtime to verify both run_next_ stealing and complete pool cleanup upon ~Runtime()
        gorun::Runtime isolated_rt(gorun::Config{.worker_count = 8});

        {
            gorun::Channel<std::shared_ptr<TrackedObject>> ptr_chan(16);
            gorun::Scope scope(isolated_rt);

            // 1. Spawn 500 routines, each capturing a std::shared_ptr<TrackedObject> in its closure
            //    and sending 20 new std::shared_ptr<TrackedObject> messages through the channel (10,000 total).
            for (int r = 0; r < 500; ++r) {
                auto closure_guard = std::make_shared<TrackedObject>(1);
                scope.spawn([&, closure_guard]() {
                    GORUN_TEST_ASSERT(closure_guard && closure_guard->payload == 1);
                    for (int m = 0; m < 20; ++m) {
                        auto msg = std::make_shared<TrackedObject>(10);
                        ptr_chan.send(std::move(msg));
                    }
                });
            }

            // 2. Spawn 50 consumers that receive all 10,000 smart pointers
            for (int c = 0; c < 50; ++c) {
                scope.spawn([&]() {
                    for (int k = 0; k < 200; ++k) {
                        auto item = ptr_chan.recv();
                        GORUN_TEST_ASSERT(item.has_value() && *item != nullptr);
                        consumed_payload_sum.fetch_add((*item)->payload, std::memory_order_relaxed);
                    }
                });
            }
        } // <-- Scope exits: all 550 routines recycle their stacks and clear `r->task = nullptr`!
    } // <-- isolated_rt is destroyed and releases all pooled stacks!

    // Verify:
    // 1. Zero lost messages across queues or run_next_ steals (10,000 * 10 == 100,000)
    GORUN_TEST_ASSERT(consumed_payload_sum.load() == 100'000);
    // 2. Exact zero live instances remaining (total_constructed == total_destroyed == 10,500)
    GORUN_TEST_ASSERT(TrackedObject::live_instances.load() == 0);
    GORUN_TEST_ASSERT(TrackedObject::total_constructed.load() == 10'500);
    GORUN_TEST_ASSERT(TrackedObject::total_destroyed.load() == 10'500);

    std::cout << "PASSED (Constructed: " << TrackedObject::total_constructed.load()
              << ", Destroyed: " << TrackedObject::total_destroyed.load()
              << ", Live Leaks: " << TrackedObject::live_instances.load() << ")\n";
}

int main() {
    std::cout << std::unitbuf;
    auto t0 = std::chrono::steady_clock::now();

    std::cout << "======================================================================\n";
    std::cout << "  gorun C++20 M:N Green Threads Runtime — 20 Comprehensive Test Suites\n";
    std::cout << "  Active OS Worker Threads: " << gorun::default_runtime().worker_count() << "\n";
    std::cout << "======================================================================\n";

    test_01_ergonomic_go_and_buffered_channel();
    test_02_unbuffered_rendezvous_channel();
    test_03_move_only_types_in_channel();
    test_04_stack_stability_invariant();
    test_05_deep_stack_recursion();
    test_06_nested_structured_scope();
    test_07_task_group_exception_and_cancellation();
    test_08_mutex_race_condition_stress();
    test_09_zero_os_thread_blocking_single_worker();
    test_10_condvar_broadcast_and_signal();
    test_11_waitgroup_multi_waiters();
    test_12_semaphore_concurrency_limit();
    test_13_call_once_race();
    test_14_rwmutex_readers_writers_consistency();
    test_15_channel_select_multiplexing();
    test_16_go_async_join_handle();
    test_17_routine_local_storage();
    test_18_lost_wakeup_and_work_stealing_stress();
    test_19_defer_lifo_and_exception_cleanup();
    test_20_zero_lost_pointers_and_lifetime_audit();

    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - t0
    ).count();

    std::cout << "======================================================================\n";
    std::cout << "  ALL 20 TEST SUITES PASSED SUCCESSFULLY in " << elapsed_ms << " ms!\n";
    std::cout << "======================================================================\n";
    return 0;
}
