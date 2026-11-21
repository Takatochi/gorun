#pragma once

/**
 * @file runtime.hpp
 * @brief Core M:N work-stealing scheduler (`Runtime`, `Worker`, `Routine`), cancellable timer thread,
 *        lock-free `RoutineLocal<T>`, and `this_routine` namespace.
 */

#include <atomic>
#include <chrono>
#include <concepts>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <random>
#include <semaphore>
#include <stop_token>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#if defined(__SANITIZE_ADDRESS__)
#define GORUN_HAS_ASAN 1
#elif defined(__has_feature)
#if __has_feature(address_sanitizer)
#define GORUN_HAS_ASAN 1
#endif
#endif

#if defined(__SANITIZE_THREAD__)
#define GORUN_HAS_TSAN 1
#elif defined(__has_feature)
#if __has_feature(thread_sanitizer)
#define GORUN_HAS_TSAN 1
#endif
#endif

#if defined(GORUN_HAS_ASAN)
extern "C" {
void __sanitizer_start_switch_fiber(void** fake_stack_save, const void* bottom, std::size_t size);
void __sanitizer_finish_switch_fiber(void* fake_stack_save, const void** bottom_old, std::size_t* size_old);
}
#endif

#if defined(GORUN_HAS_TSAN)
extern "C" {
void* __tsan_get_current_fiber(void);
void* __tsan_create_fiber(unsigned flags);
void  __tsan_destroy_fiber(void* fiber);
void  __tsan_switch_to_fiber(void* fiber, unsigned flags);
}
#endif

#include "gorun/detail/context.hpp"
#include "gorun/detail/spinlock.hpp"
#include "gorun/detail/stack_pool.hpp"
#include "gorun/detail/work_stealing_deque.hpp"

namespace gorun {

class Runtime;
class TaskGroup;

/**
 * @struct Config
 * @brief Configuration options for a `gorun::Runtime` instance.
 */
struct Config {
    /**
     * @brief Number of OS worker threads (`std::jthread`) multiplexing green routines.
     * Defaults to `std::thread::hardware_concurrency()` (analogous to `GOMAXPROCS` in Go).
     */
    std::size_t worker_count = std::thread::hardware_concurrency() > 0
                                   ? std::thread::hardware_concurrency()
                                   : 4;

    /**
     * @brief Usable virtual-memory stack size per green routine in bytes (default: 128 KB, minimum 16 KB).
     */
    std::size_t default_stack_size = 128 * 1024;

    /**
     * @brief Maximum number of idle routine stacks cached in `StackPool` for zero-syscall reuse.
     */
    std::size_t max_pooled_stacks = 1024;
};

/**
 * @struct RuntimeStats
 * @brief Live scheduler introspection snapshot returned by `Runtime::stats()` (Prometheus/Grafana ready).
 */
struct RuntimeStats {
    std::size_t worker_count{0};            ///< Total OS worker threads
    std::size_t sleeping_workers{0};        ///< OS worker threads currently parked on idle CV
    std::size_t active_routines{0};         ///< Unfinished routines (Ready + Running + Parked)
    std::size_t parked_routines{0};         ///< Routines currently parked on Channel/Mutex/Timer/NetPoller
    std::size_t running_routines{0};        ///< Routines actively executing on worker threads right now
    std::size_t global_queue_size{0};       ///< Routines waiting in the Global Run Queue
    std::vector<std::size_t> local_queue_sizes{}; ///< Per-worker local deque sizes
    std::size_t total_local_queued{0};      ///< Sum of all per-worker local deque sizes
    std::uint64_t total_spawned{0};         ///< Lifetime routines spawned on this Runtime
    std::uint64_t total_completed{0};       ///< Lifetime routines completed on this Runtime
    std::uint64_t total_steals{0};          ///< Lifetime successful work-stealing events across workers
    std::size_t pending_timers{0};          ///< Currently armed timers in the timer wheel/map
    StackPoolStats stack_pool{};            ///< Multi-bucket StackPool allocations, cache hits & cached bytes
};

namespace detail {

class Worker;

/**
 * @class UniqueTask
 * @brief C++20 move-only type-erased callable wrapper (`void()`) with 32-byte Small Buffer Optimization (SBO),
 *        enabling zero-allocation capture of typical lambdas and move-only lambdas (e.g., `std::unique_ptr<T>`)
 *        in `gorun::go(...)`, `TaskGroup::spawn(...)`, and `go_async(...)`.
 */
class UniqueTask {
    static constexpr std::size_t kInlineSize = 32;

    struct VTable {
        void (*invoke)(void* storage);
        void (*move_to)(void* dst, void* src) noexcept;
        void (*destroy)(void* storage) noexcept;
    };

    template <typename F>
    static constexpr bool kFitsInline =
        sizeof(F) <= kInlineSize &&
        alignof(F) <= alignof(std::max_align_t) &&
        std::is_nothrow_move_constructible_v<F>;

    template <typename F>
    static const VTable* vtable_for() noexcept {
        if constexpr (kFitsInline<F>) {
            static constexpr VTable vt{
                [](void* s) { (*static_cast<F*>(s))(); },
                [](void* dst, void* src) noexcept {
                    ::new (dst) F(std::move(*static_cast<F*>(src)));
                    static_cast<F*>(src)->~F();
                },
                [](void* s) noexcept { static_cast<F*>(s)->~F(); }
            };
            return &vt;
        } else {
            static constexpr VTable vt{
                [](void* s) {
                    F* p = nullptr;
                    std::memcpy(&p, s, sizeof(p));
                    (*p)();
                },
                [](void* dst, void* src) noexcept {
                    std::memcpy(dst, src, sizeof(F*));
                },
                [](void* s) noexcept {
                    F* p = nullptr;
                    std::memcpy(&p, s, sizeof(p));
                    delete p;
                }
            };
            return &vt;
        }
    }

    alignas(std::max_align_t) std::byte storage_[kInlineSize];
    const VTable* vtable_{nullptr};

public:
    UniqueTask() noexcept = default;
    UniqueTask(std::nullptr_t) noexcept {}

    template <typename F>
        requires (!std::same_as<std::decay_t<F>, UniqueTask> && std::invocable<std::decay_t<F>&>)
    UniqueTask(F&& f) {
        using Decayed = std::decay_t<F>;
        if constexpr (kFitsInline<Decayed>) {
            ::new (static_cast<void*>(storage_)) Decayed(std::forward<F>(f));
        } else {
            Decayed* p = new Decayed(std::forward<F>(f));
            std::memcpy(storage_, &p, sizeof(p));
        }
        vtable_ = vtable_for<Decayed>();
    }

    UniqueTask(UniqueTask&& other) noexcept {
        if (other.vtable_) {
            other.vtable_->move_to(storage_, other.storage_);
            vtable_ = std::exchange(other.vtable_, nullptr);
        }
    }

    UniqueTask& operator=(UniqueTask&& other) noexcept {
        if (this != &other) {
            reset();
            if (other.vtable_) {
                other.vtable_->move_to(storage_, other.storage_);
                vtable_ = std::exchange(other.vtable_, nullptr);
            }
        }
        return *this;
    }

    UniqueTask& operator=(std::nullptr_t) noexcept {
        reset();
        return *this;
    }

    ~UniqueTask() noexcept { reset(); }

    UniqueTask(const UniqueTask&) = delete;
    UniqueTask& operator=(const UniqueTask&) = delete;

    void reset() noexcept {
        if (vtable_) {
            vtable_->destroy(storage_);
            vtable_ = nullptr;
        }
    }

    explicit operator bool() const noexcept { return vtable_ != nullptr; }

    void operator()() {
        if (vtable_) {
            vtable_->invoke(storage_);
        }
    }
};

/**
 * @enum RoutineState
 * @brief Lifecycle states of a green routine (analogous to `_Grunnable`, `_Grunning`, `_Gwaiting`, `_Gdead` in Go).
 */
enum class RoutineState : std::uint8_t {
    Ready,    ///< Routine is runnable and queued in a local or global run queue
    Running,  ///< Routine is currently executing on an OS `Worker` thread
    Parked,   ///< Routine is suspended on a synchronization primitive or timer (consumes zero OS threads)
    Finished  ///< Routine body has returned; stack and descriptor are ready for recycling
};

/**
 * @struct Routine
 * @brief Internal representation of a lightweight green thread (analogous to `runtime.g` in Go).
 */
struct Routine {
    struct LocalSlot {
        void* ptr{nullptr};
        void (*deleter)(void*){nullptr};

        LocalSlot() = default;
        LocalSlot(void* p, void (*d)(void*)) noexcept : ptr(p), deleter(d) {}
        LocalSlot(const LocalSlot&) = delete;
        LocalSlot& operator=(const LocalSlot&) = delete;
        LocalSlot(LocalSlot&& other) noexcept
            : ptr(std::exchange(other.ptr, nullptr))
            , deleter(std::exchange(other.deleter, nullptr)) {}
        LocalSlot& operator=(LocalSlot&& other) noexcept {
            if (this != &other) {
                reset();
                ptr = std::exchange(other.ptr, nullptr);
                deleter = std::exchange(other.deleter, nullptr);
            }
            return *this;
        }
        ~LocalSlot() noexcept { reset(); }

        void reset() noexcept {
            if (ptr && deleter) {
                deleter(ptr);
            }
            ptr = nullptr;
            deleter = nullptr;
        }
    };

    std::uint64_t id{0};                        ///< Unique monotonically increasing routine ID
    RoutineState state{RoutineState::Ready};    ///< Current lifecycle state
    ExecutionContext ctx{};                     ///< Saved CPU registers and stack pointer (`%rsp`)
    StackAllocation stack{};                    ///< Virtual-memory stack region with hardware Guard Page
    UniqueTask task{};                          ///< Move-only user callable body
    std::stop_source stop_source{};             ///< Cooperative cancellation source for this routine
    Runtime* owner_runtime{nullptr};            ///< Owning `Runtime` instance
    TaskGroup* implicit_scope{nullptr};         ///< Active structured concurrency `TaskGroup` / `Scope`

    // Per-routine storage for RoutineLocal<T> — automatically destroyed when this routine finishes (zero leaks!)
    std::unordered_map<std::uint64_t, LocalSlot> routine_locals{};

    Routine* prev{nullptr};
    Routine* next{nullptr};
#if defined(GORUN_HAS_ASAN)
    void* asan_fake_stack{nullptr};
#endif
#if defined(GORUN_HAS_TSAN)
    void* tsan_fiber{nullptr};
#endif

    ~Routine() noexcept {
#if defined(GORUN_HAS_TSAN)
        if (tsan_fiber) {
            __tsan_destroy_fiber(tsan_fiber);
            tsan_fiber = nullptr;
        }
#endif
    }

    void clear_locals() noexcept {
        routine_locals.clear();
    }
};

inline Worker*& tls_current_worker() noexcept {
    thread_local Worker* w = nullptr;
    return w;
}

inline Routine*& tls_current_routine() noexcept {
    thread_local Routine* r = nullptr;
    return r;
}

inline TaskGroup*& tls_implicit_scope() noexcept {
    thread_local TaskGroup* s = nullptr;
    return s;
}

/**
 * @brief Returns the `Worker` bound to the current OS thread, or `nullptr` if called from an external thread (e.g. `main()`).
 */
#if defined(__GNUC__) || defined(__clang__)
__attribute__((noinline))
#elif defined(_MSC_VER)
__declspec(noinline)
#endif
inline Worker* current_worker() noexcept {
    std::atomic_signal_fence(std::memory_order_seq_cst);
    return tls_current_worker();
}

/**
 * @brief Returns the green `Routine` currently running on this OS worker thread, or `nullptr`.
 */
#if defined(__GNUC__) || defined(__clang__)
__attribute__((noinline))
#elif defined(_MSC_VER)
__declspec(noinline)
#endif
inline Routine* current_routine() noexcept {
    std::atomic_signal_fence(std::memory_order_seq_cst);
    return tls_current_routine();
}

/**
 * @brief Returns the active structured concurrency `TaskGroup` (or `Scope`) for the current routine or OS thread.
 */
inline TaskGroup* current_implicit_scope() noexcept {
    if (Routine* r = current_routine()) {
        return r->implicit_scope;
    }
    return tls_implicit_scope();
}

/**
 * @brief Sets the active structured concurrency `TaskGroup` for the current routine or OS thread, returning the previous one.
 */
inline TaskGroup* set_implicit_scope(TaskGroup* s) noexcept {
    if (Routine* r = current_routine()) {
        return std::exchange(r->implicit_scope, s);
    }
    return std::exchange(tls_implicit_scope(), s);
}

/**
 * @struct UniversalWaiter
 * @brief Hybrid suspension node allowing `gorun` primitives (`Channel`, `Mutex`, `WaitGroup`) to be called
 *        seamlessly from both green routines and external OS threads (such as `int main()`).
 *
 * - **Inside a Green Routine (`current_worker() != nullptr`):** Suspends the routine via `Worker::park_locked()`
 *   using a user-space `context_switch` (**Zero OS-thread blocking**).
 * - **From an External OS Thread (`current_worker() == nullptr`):** Safely parks the calling OS thread on a
 *   stack-allocated `std::binary_semaphore`.
 */
struct UniversalWaiter {
    Routine* routine{nullptr};
    std::binary_semaphore* os_sem{nullptr};

    UniversalWaiter* prev{nullptr};
    UniversalWaiter* next{nullptr};

    static UniversalWaiter make_for_current(std::binary_semaphore& fallback_sem) noexcept {
        Routine* r = current_routine();
        return UniversalWaiter{
            .routine = r,
            .os_sem = (r == nullptr) ? &fallback_sem : nullptr
        };
    }

    /**
     * @brief Atomically parks the current green routine (or external OS thread) and releases `primitive_lock`.
     */
    void park_current(SpinLock& primitive_lock) noexcept;

    /**
     * @brief Unparks the waiting routine (re-schedules it onto a worker run queue) or wakes the external OS thread.
     */
    void unpark() noexcept;
};

} // namespace detail

/**
 * @class Runtime
 * @brief M:N work-stealing green thread runtime.
 *
 * Manages the pool of OS `Worker` threads, the `Global Run Queue`, the `StackPool`, and the background
 * cancellable `Timer Thread`. Can be instantiated explicitly for isolated pools (`gorun::Runtime rt(cfg);`)
 * or accessed implicitly via the lazy global singleton `gorun::default_runtime()` whenever `gorun::go(...)` is called.
 */
class Runtime {
private:
    friend class detail::Worker;
    friend struct detail::UniversalWaiter;

    Config config_;
    detail::StackPool stack_pool_;
    std::atomic<std::uint64_t> next_routine_id_{1};

    detail::WorkStealingDeque<detail::Routine*> global_queue_;

    std::vector<std::unique_ptr<detail::Worker>> workers_;
    std::vector<std::jthread> worker_threads_;

    std::mutex sleep_mutex_;
    std::condition_variable sleep_cv_;
    std::atomic<std::size_t> sleeping_workers_{0};
    std::atomic<std::uint32_t> waking_workers_{0};
    std::atomic<std::size_t> active_routines_{0};
    std::atomic<std::size_t> parked_routines_{0};
    std::atomic<std::size_t> running_routines_{0};
    std::atomic<std::uint64_t> total_spawned_{0};
    std::atomic<std::uint64_t> total_completed_{0};
    std::atomic<std::uint64_t> total_steals_{0};
    std::atomic<bool> shutdown_{false};

    // Cancellable timer queue (O(log N) insert, O(1) earliest peek, O(log N) cancel with zero timer leaks!)
    struct TimerEntry {
        std::uint64_t id{0};
        detail::Routine* routine{nullptr};
        detail::UniqueTask callback{};
    };

    using TimerMap = std::multimap<std::chrono::steady_clock::time_point, TimerEntry>;

    std::atomic<std::uint64_t> next_timer_id_{1};
    std::mutex timer_mutex_;
    std::condition_variable timer_cv_;
    TimerMap timers_;
    std::unordered_map<std::uint64_t, TimerMap::iterator> timer_index_;
    std::jthread timer_thread_;

    std::mutex routine_pool_mutex_;
    std::vector<std::unique_ptr<detail::Routine>> routine_pool_;

    static void routine_trampoline(void* arg) noexcept;

    detail::Routine* acquire_routine(detail::UniqueTask fn, std::size_t requested_stack_size = 0);
    void release_routine(detail::Routine* r) noexcept;
    void timer_loop();

public:
    /**
     * @brief Constructs and starts the M:N runtime with the given configuration.
     * @param cfg Configuration parameters (worker thread count, stack size, pool capacity).
     */
    explicit Runtime(Config cfg = {});

    /**
     * @brief Waits for all active routines to finish (`wait_idle()`) and cleanly joins all worker and timer threads.
     */
    ~Runtime();

    Runtime(const Runtime&) = delete;
    Runtime& operator=(const Runtime&) = delete;

    /**
     * @brief Schedules a `Ready` routine onto the current worker's local deque (if on a worker of this runtime)
     *        or onto the `Global Run Queue`, waking an idle worker thread if necessary.
     * @param r Pointer to the routine to schedule.
     */
    void schedule(detail::Routine* r) noexcept;

    /**
     * @brief Registers a cancellable timer deadline in the timer thread to wake routine `r`.
     * @return Unique non-zero timer ID that may be passed to `cancel_timer()`.
     */
    std::uint64_t schedule_timer(std::chrono::steady_clock::time_point deadline, detail::Routine* r);

    /**
     * @brief Registers a cancellable callback in the timer thread at `deadline`.
     * @return Unique non-zero timer ID that may be passed to `cancel_timer()`.
     */
    std::uint64_t schedule_timer_callback(std::chrono::steady_clock::time_point deadline, detail::UniqueTask cb);

    /**
     * @brief Cancels a pending timer by its `timer_id`, immediately reclaiming its memory from the timer map.
     * @return `true` if the timer was found and cancelled before firing, `false` if already fired or unknown.
     */
    bool cancel_timer(std::uint64_t timer_id) noexcept;

    /**
     * @brief Spawns a new green routine on this `Runtime` instance (supports move-only callables and custom stack sizes).
     * @tparam F Callable type `void()`.
     * @param fn Callable task body.
     * @param stack_size Optional usable stack size in bytes (`0` = `Config::default_stack_size`).
     */
    template <typename F>
    void spawn_raw(F&& fn, std::size_t stack_size = 0) {
        // Acquire routine BEFORE incrementing active_routines_ so std::bad_alloc never deadlocks wait_idle()!
        detail::Routine* r = acquire_routine(detail::UniqueTask(std::forward<F>(fn)), stack_size);
        r->implicit_scope = detail::current_implicit_scope();
        active_routines_.fetch_add(1, std::memory_order_relaxed);
        total_spawned_.fetch_add(1, std::memory_order_relaxed);
        schedule(r);
    }

    /**
     * @brief Spawns a green routine on this `Runtime` instance.
     */
    template <typename F>
    void go(F&& fn) {
        spawn_raw(std::forward<F>(fn), 0);
    }

    /**
     * @brief Spawns a green routine with a predefined `StackSize` (`Small`, `Compact`, `Default`, `Medium`, `Large`, `Huge`).
     */
    template <typename F>
    void go(StackSize sz, F&& fn) {
        spawn_raw(std::forward<F>(fn), static_cast<std::size_t>(sz));
    }

    /**
     * @brief Spawns a green routine with explicit `Options` (e.g. `Options{.stack_size = 512 * 1024}`).
     */
    template <typename F>
    void go(Options opts, F&& fn) {
        spawn_raw(std::forward<F>(fn), opts.stack_size);
    }

    /**
     * @brief Spawns `fn` as a green routine on this runtime and waits until it completes.
     *
     * Safe to call from BOTH external OS threads (`main()`) and inside existing green routines
     * (parks the calling green routine via `UniversalWaiter` instead of blocking the OS worker thread!).
     * Any uncaught exception thrown inside `fn` is captured into `std::exception_ptr` and rethrown.
     *
     * @tparam F Nullary callable type.
     * @param fn Entry function to execute inside the runtime.
     * @return The result returned by `fn()`.
     */
    template <std::invocable F>
    auto block_on(F&& fn) -> std::invoke_result_t<F>;

    /**
     * @brief Blocks the calling thread until all active routines in this runtime have finished executing.
     */
    void wait_idle() noexcept;

    /**
     * @brief Returns the number of OS worker threads (`Worker`) in this runtime.
     */
    [[nodiscard]] std::size_t worker_count() const noexcept {
        return config_.worker_count;
    }

    /**
     * @brief Returns the current number of active (unfinished) green routines.
     */
    [[nodiscard]] std::size_t active_routine_count() const noexcept {
        return active_routines_.load(std::memory_order_acquire);
    }

    /**
     * @brief Returns the current number of pending (uncancelled) timers in the timer thread.
     */
    [[nodiscard]] std::size_t pending_timer_count() noexcept {
        std::lock_guard lock(timer_mutex_);
        return timer_index_.size();
    }

    /**
     * @brief Captures a live observability snapshot of scheduler queues, active/parked routines,
     *        work-stealing steals, timers, and multi-bucket `StackPool` usage.
     */
    [[nodiscard]] RuntimeStats stats() noexcept;
};

/**
 * @brief Returns the lazy global default `Runtime` instance.
 *
 * Automatically initialized on the first call to `gorun::go(...)` and cleanly shut down at program exit.
 */
inline Runtime& default_runtime() {
    static Runtime instance{};
    return instance;
}

namespace detail {

/**
 * @class Worker
 * @brief OS worker thread combining the roles of `M` (OS thread) and `P` (logical processor) from Go's runtime.
 *
 * Owns a local `WorkStealingDeque<Routine*>`, a scheduler execution context `scheduler_ctx_`,
 * and implements the atomic `park_locked()` post-switch unlock protocol to prevent Lost Wakeups and stack races.
 */
class Worker {
    friend class ::gorun::Runtime;

    Runtime& runtime_;
    std::size_t index_;
    ExecutionContext scheduler_ctx_{};
    WorkStealingDeque<Routine*> local_queue_{256};

    Routine* current_routine_{nullptr};
    std::vector<std::unique_ptr<Routine>> local_free_routines_{}; ///< Per-worker lock-free routine cache (Go's `p.gFree`)
    SpinLock* pending_unlock_{nullptr};
    bool yield_requested_{false};
    bool finish_requested_{false};
    std::uint64_t tick_{0};
    std::mt19937 rng_;
#if defined(GORUN_HAS_ASAN)
    void* asan_scheduler_fake_stack_{nullptr};
    const void* asan_scheduler_bottom_{nullptr};
    std::size_t asan_scheduler_size_{0};
#endif
#if defined(GORUN_HAS_TSAN)
    void* tsan_scheduler_fiber_{nullptr};
#endif

public:
    Worker(Runtime& rt, std::size_t idx)
        : runtime_(rt)
        , index_(idx)
        , rng_(static_cast<std::uint32_t>(idx * 2654435761u + 17u)) {}

    [[nodiscard]] Runtime& runtime() noexcept { return runtime_; }
    [[nodiscard]] std::size_t index() const noexcept { return index_; }
    [[nodiscard]] ExecutionContext& scheduler_context() noexcept { return scheduler_ctx_; }
    [[nodiscard]] std::vector<std::unique_ptr<Routine>>& local_free_routines() noexcept { return local_free_routines_; }
    [[nodiscard]] std::size_t local_queue_size() const noexcept { return local_queue_.size(); }

#if defined(GORUN_HAS_ASAN)
    void asan_before_switch_to_routine(Routine* r) noexcept {
        __sanitizer_start_switch_fiber(&asan_scheduler_fake_stack_, r->stack.stack_bottom, r->stack.usable_bytes);
    }

    void asan_after_switch_to_routine(Routine* r) noexcept {
        __sanitizer_finish_switch_fiber(r->asan_fake_stack, &asan_scheduler_bottom_, &asan_scheduler_size_);
    }

    void asan_before_switch_to_scheduler(Routine* r, bool is_finishing) noexcept {
        __sanitizer_start_switch_fiber(
            is_finishing ? nullptr : &r->asan_fake_stack,
            asan_scheduler_bottom_,
            asan_scheduler_size_
        );
    }

    void asan_after_switch_to_scheduler() noexcept {
        const void* dummy_bottom = nullptr;
        std::size_t dummy_size = 0;
        __sanitizer_finish_switch_fiber(asan_scheduler_fake_stack_, &dummy_bottom, &dummy_size);
    }
#endif

    void park_locked(SpinLock& primitive_lock) noexcept {
        Routine* r = current_routine_;
        r->state = RoutineState::Parked;
        runtime_.parked_routines_.fetch_add(1, std::memory_order_relaxed);
        pending_unlock_ = &primitive_lock;
#if defined(GORUN_HAS_ASAN)
        asan_before_switch_to_scheduler(r, false);
#endif
#if defined(GORUN_HAS_TSAN)
        __tsan_switch_to_fiber(tsan_scheduler_fiber_, 0);
#endif
        context_switch(&r->ctx, &scheduler_ctx_);
#if defined(GORUN_HAS_ASAN)
        current_worker()->asan_after_switch_to_routine(r);
#endif
    }

    void yield_current() noexcept {
        Routine* r = current_routine_;
        r->state = RoutineState::Ready;
        yield_requested_ = true;
#if defined(GORUN_HAS_ASAN)
        asan_before_switch_to_scheduler(r, false);
#endif
#if defined(GORUN_HAS_TSAN)
        __tsan_switch_to_fiber(tsan_scheduler_fiber_, 0);
#endif
        context_switch(&r->ctx, &scheduler_ctx_);
#if defined(GORUN_HAS_ASAN)
        current_worker()->asan_after_switch_to_routine(r);
#endif
    }

    void finish_current() noexcept {
        Routine* r = current_routine_;
        r->state = RoutineState::Finished;
        finish_requested_ = true;
#if defined(GORUN_HAS_ASAN)
        asan_before_switch_to_scheduler(r, false);
#endif
#if defined(GORUN_HAS_TSAN)
        __tsan_switch_to_fiber(tsan_scheduler_fiber_, 0);
#endif
        context_switch(&r->ctx, &scheduler_ctx_);
#if defined(GORUN_HAS_ASAN)
        current_worker()->asan_after_switch_to_routine(r);
#endif
    }

    /**
     * @brief Transitions a parked routine `r` to `Ready` and schedules it for execution.
     */
    void unpark(Routine* r) noexcept {
        runtime_.schedule(r);
    }

    /**
     * @brief Selects the next runnable routine following Go's M:N scheduler polling order:
     * 1. Every 61st tick: poll `Global Run Queue` first to guarantee fairness and prevent starvation.
     * 2. Pop from local worker deque `local_queue_.pop_back()` (LIFO for L1/L2 cache locality).
     * 3. Pop from `Global Run Queue` (FIFO).
     * 4. Work-Stealing: attempt to steal half of the tasks (`steal_half_into`) from a randomly chosen victim worker.
     */
    Routine* find_runnable() {
        ++tick_;

        if ((tick_ % 61) == 0) {
            if (Routine* r = runtime_.global_queue_.pop_front()) {
                return r;
            }
        }

        if (Routine* r = local_queue_.pop_back()) {
            return r;
        }

        if (Routine* r = runtime_.global_queue_.pop_front()) {
            return r;
        }

        const std::size_t num_workers = runtime_.workers_.size();
        if (num_workers > 1) {
            std::size_t start = rng_() % num_workers;
            for (std::size_t i = 0; i < num_workers; ++i) {
                std::size_t victim_idx = (start + i) % num_workers;
                if (victim_idx == index_) continue;
                Worker* victim = runtime_.workers_[victim_idx].get();
                if (Routine* stolen = victim->local_queue_.steal_half_into(local_queue_)) {
                    runtime_.total_steals_.fetch_add(1, std::memory_order_relaxed);
                    return stolen;
                }
            }
        }

        return nullptr;
    }

    /**
     * @brief Main execution loop for the OS worker thread.
     */
    void run_loop() {
        tls_current_worker() = this;
        scheduler_ctx_.init_scheduler_for_current_thread();
#if defined(GORUN_HAS_TSAN)
        tsan_scheduler_fiber_ = __tsan_get_current_fiber();
#endif

        while (!runtime_.shutdown_.load(std::memory_order_acquire)) {
            Routine* r = find_runnable();
            if (!r) {
                // Short active spin before blocking the OS thread on condition_variable
                for (int spin = 0; spin < 64; ++spin) {
                    cpu_relax();
                    if ((r = find_runnable()) != nullptr) {
                        break;
                    }
                }
            }
            if (!r) {
                std::unique_lock lock(runtime_.sleep_mutex_);
                runtime_.sleeping_workers_.fetch_add(1, std::memory_order_seq_cst);
                if (runtime_.shutdown_.load(std::memory_order_acquire) ||
                    !runtime_.global_queue_.empty() || !local_queue_.empty()) {
                    runtime_.sleeping_workers_.fetch_sub(1, std::memory_order_seq_cst);
                    runtime_.waking_workers_.store(0, std::memory_order_release);
                    continue;
                }
                // Check if any sibling worker has stealable work before sleeping
                bool sibling_has_work = false;
                for (const auto& w : runtime_.workers_) {
                    if (w.get() != this && !w->local_queue_.empty()) {
                        sibling_has_work = true;
                        break;
                    }
                }
                if (sibling_has_work) {
                    runtime_.sleeping_workers_.fetch_sub(1, std::memory_order_seq_cst);
                    runtime_.waking_workers_.store(0, std::memory_order_release);
                    continue;
                }
                runtime_.sleep_cv_.wait_for(lock, std::chrono::milliseconds(50));
                runtime_.sleeping_workers_.fetch_sub(1, std::memory_order_seq_cst);
                runtime_.waking_workers_.store(0, std::memory_order_release);
                if (!runtime_.global_queue_.empty() &&
                    runtime_.sleeping_workers_.load(std::memory_order_relaxed) > 0) {
                    runtime_.sleep_cv_.notify_one();
                }
                continue;
            }

            current_routine_ = r;
            tls_current_routine() = r;
            r->state = RoutineState::Running;
            runtime_.running_routines_.fetch_add(1, std::memory_order_relaxed);

#if defined(GORUN_HAS_ASAN)
            asan_before_switch_to_routine(r);
#endif
#if defined(GORUN_HAS_TSAN)
            __tsan_switch_to_fiber(r->tsan_fiber, 0);
#endif
            context_switch(&scheduler_ctx_, &r->ctx);
#if defined(GORUN_HAS_ASAN)
            asan_after_switch_to_scheduler();
#endif

            runtime_.running_routines_.fetch_sub(1, std::memory_order_relaxed);
            current_routine_ = nullptr;
            tls_current_routine() = nullptr;

            if (pending_unlock_) {
                SpinLock* lk = std::exchange(pending_unlock_, nullptr);
                lk->unlock();
            }

            if (yield_requested_) {
                yield_requested_ = false;
                // Push to FIFO head so other queued local routines run before this routine (prevents LIFO starvation)
                local_queue_.push_front(r);
            } else if (finish_requested_) {
                finish_requested_ = false;
                runtime_.release_routine(r);
                runtime_.total_completed_.fetch_add(1, std::memory_order_relaxed);
                if (runtime_.active_routines_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                    std::lock_guard lk(runtime_.sleep_mutex_);
                    runtime_.sleep_cv_.notify_all();
                }
            }
        }

        scheduler_ctx_.cleanup_scheduler_for_current_thread();
        tls_current_worker() = nullptr;
    }
};

inline void UniversalWaiter::park_current(SpinLock& primitive_lock) noexcept {
    if (Worker* w = current_worker()) {
        w->park_locked(primitive_lock);
    } else {
        primitive_lock.unlock();
        os_sem->acquire();
    }
}

inline void UniversalWaiter::unpark() noexcept {
    if (routine) {
        if (routine->owner_runtime) {
            routine->owner_runtime->schedule(routine);
        } else {
            default_runtime().schedule(routine);
        }
    } else if (os_sem) {
        os_sem->release();
    }
}

} // namespace detail

inline void Runtime::routine_trampoline(void* arg) noexcept {
    auto* r = static_cast<detail::Routine*>(arg);
#if defined(GORUN_HAS_ASAN)
    detail::current_worker()->asan_after_switch_to_routine(r);
#endif
    for (;;) {
        try {
            if (r->task) {
                r->task();
            }
        } catch (...) {
        }
        // Destroy captured closure & all RoutineLocal<T> instances immediately before recycling!
        r->task = nullptr;
        r->clear_locals();
        detail::current_worker()->finish_current();
    }
}

inline detail::Routine* Runtime::acquire_routine(detail::UniqueTask fn, std::size_t requested_stack_size) {
    const std::size_t default_usable = stack_pool_.default_usable_size();
    const std::size_t target_usable = (requested_stack_size == 0)
        ? default_usable
        : detail::StackPool::normalize_size(requested_stack_size);
    const bool is_default_size = (target_usable == default_usable);

    std::unique_ptr<detail::Routine> r;
    if (is_default_size) {
        detail::Worker* w = detail::current_worker();
        if (w && &w->runtime() == this && !w->local_free_routines().empty()) {
            r = std::move(w->local_free_routines().back());
            w->local_free_routines().pop_back();
        } else {
            std::lock_guard lock(routine_pool_mutex_);
            if (!routine_pool_.empty()) {
                r = std::move(routine_pool_.back());
                routine_pool_.pop_back();
            }
        }
    }

    if (!r) {
        r = std::make_unique<detail::Routine>();
        r->owner_runtime = this;
        r->stack = stack_pool_.acquire(target_usable);
        r->ctx.make_routine_stack_context(r->stack, &Runtime::routine_trampoline, r.get());
#if defined(GORUN_HAS_TSAN)
        r->tsan_fiber = __tsan_create_fiber(0);
#endif
    }

    r->id = next_routine_id_.fetch_add(1, std::memory_order_relaxed);
    r->state = detail::RoutineState::Ready;
    r->task = std::move(fn);
    r->stop_source = std::stop_source{};
    r->implicit_scope = nullptr;
    return r.release();
}

inline void Runtime::release_routine(detail::Routine* r) noexcept {
    std::unique_ptr<detail::Routine> ptr(r);
    const bool is_default_size = (ptr->stack.usable_bytes == stack_pool_.default_usable_size());
    if (is_default_size) {
        detail::Worker* w = detail::current_worker();
        if (w && &w->runtime() == this && w->local_free_routines().size() < 64) {
            w->local_free_routines().push_back(std::move(ptr));
            return;
        }
        {
            std::lock_guard lock(routine_pool_mutex_);
            if (routine_pool_.size() < config_.max_pooled_stacks) {
                routine_pool_.push_back(std::move(ptr));
                return;
            }
        }
    }
    // Custom-sized stacks return directly to their matching bucket in StackPool!
    stack_pool_.release(ptr->stack);
}

inline Runtime::Runtime(Config cfg)
    : config_(cfg)
    , stack_pool_(cfg.default_stack_size, cfg.max_pooled_stacks) {
    if (config_.worker_count == 0) {
        config_.worker_count = 1;
    }

    workers_.reserve(config_.worker_count);
    for (std::size_t i = 0; i < config_.worker_count; ++i) {
        workers_.push_back(std::make_unique<detail::Worker>(*this, i));
    }

    timer_thread_ = std::jthread([this]() { timer_loop(); });

    worker_threads_.reserve(config_.worker_count);
    for (std::size_t i = 0; i < config_.worker_count; ++i) {
        detail::Worker* w = workers_[i].get();
        worker_threads_.emplace_back([w]() { w->run_loop(); });
    }
}

inline Runtime::~Runtime() {
    wait_idle();

    shutdown_.store(true, std::memory_order_release);
    {
        std::lock_guard lk(sleep_mutex_);
        sleep_cv_.notify_all();
    }
    {
        std::lock_guard lk(timer_mutex_);
        timer_cv_.notify_all();
    }

    for (auto& t : worker_threads_) {
        if (t.joinable()) {
            t.join();
        }
    }
    if (timer_thread_.joinable()) {
        timer_thread_.join();
    }

    std::lock_guard lock(routine_pool_mutex_);
    for (auto& w : workers_) {
        for (auto& r : w->local_free_routines()) {
            stack_pool_.release(r->stack);
        }
        w->local_free_routines().clear();
    }
    for (auto& r : routine_pool_) {
        stack_pool_.release(r->stack);
    }
    routine_pool_.clear();
}

inline void Runtime::schedule(detail::Routine* r) noexcept {
    if (r->state == detail::RoutineState::Parked) {
        parked_routines_.fetch_sub(1, std::memory_order_relaxed);
    }
    r->state = detail::RoutineState::Ready;
    bool pushed_to_global = false;
    bool should_wake = false;
    detail::Worker* w = detail::current_worker();
    if (w && &w->runtime() == this) {
        // Only wake an idle sibling core if this worker now has >=2 local runnable routines;
        // keeping single-task producer-consumer handoffs in run_next_ preserves L1 cache locality.
        should_wake = w->local_queue_.push_run_next(r);
    } else {
        global_queue_.push_back(r);
        pushed_to_global = true;
        should_wake = true;
    }

    if (should_wake) {
        // Full fence pairs with sleeping_workers_.fetch_add(seq_cst) in Worker::run_loop() to prevent lost wakeups!
        std::atomic_thread_fence(std::memory_order_seq_cst);
        if (sleeping_workers_.load(std::memory_order_seq_cst) > 0) {
            if (pushed_to_global || waking_workers_.exchange(1, std::memory_order_acq_rel) == 0) {
                std::lock_guard lk(sleep_mutex_);
                sleep_cv_.notify_one();
            }
        }
    }
}

inline std::uint64_t Runtime::schedule_timer(std::chrono::steady_clock::time_point deadline, detail::Routine* r) {
    std::uint64_t id = next_timer_id_.fetch_add(1, std::memory_order_relaxed);
    bool is_earliest = false;
    {
        std::lock_guard lock(timer_mutex_);
        auto it = timers_.emplace(deadline, TimerEntry{.id = id, .routine = r, .callback = nullptr});
        timer_index_.emplace(id, it);
        is_earliest = (it == timers_.begin());
    }
    if (is_earliest) {
        timer_cv_.notify_one();
    }
    return id;
}

inline std::uint64_t Runtime::schedule_timer_callback(std::chrono::steady_clock::time_point deadline, detail::UniqueTask cb) {
    std::uint64_t id = next_timer_id_.fetch_add(1, std::memory_order_relaxed);
    bool is_earliest = false;
    {
        std::lock_guard lock(timer_mutex_);
        auto it = timers_.emplace(deadline, TimerEntry{.id = id, .routine = nullptr, .callback = std::move(cb)});
        timer_index_.emplace(id, it);
        is_earliest = (it == timers_.begin());
    }
    if (is_earliest) {
        timer_cv_.notify_one();
    }
    return id;
}

inline bool Runtime::cancel_timer(std::uint64_t timer_id) noexcept {
    if (timer_id == 0) {
        return false;
    }
    std::lock_guard lock(timer_mutex_);
    auto idx_it = timer_index_.find(timer_id);
    if (idx_it == timer_index_.end()) {
        return false;
    }
    timers_.erase(idx_it->second);
    timer_index_.erase(idx_it);
    return true;
}

inline void Runtime::timer_loop() {
    std::unique_lock lock(timer_mutex_);
    while (!shutdown_.load(std::memory_order_acquire)) {
        if (timers_.empty()) {
            timer_cv_.wait(lock, [this]() {
                return shutdown_.load(std::memory_order_acquire) || !timers_.empty();
            });
            continue;
        }

        auto now = std::chrono::steady_clock::now();
        auto earliest = timers_.begin();
        if (earliest->first <= now) {
            TimerEntry entry = std::move(earliest->second);
            timer_index_.erase(entry.id);
            timers_.erase(earliest);
            lock.unlock();

            if (entry.routine) {
                schedule(entry.routine);
            } else if (entry.callback) {
                entry.callback();
            }

            lock.lock();
        } else {
            auto next_deadline = earliest->first;
            timer_cv_.wait_until(lock, next_deadline);
        }
    }
}

inline void Runtime::wait_idle() noexcept {
    std::unique_lock lock(sleep_mutex_);
    sleep_cv_.wait(lock, [this]() {
        return active_routines_.load(std::memory_order_acquire) == 0;
    });
}

inline RuntimeStats Runtime::stats() noexcept {
    RuntimeStats s;
    s.worker_count = config_.worker_count;
    s.sleeping_workers = sleeping_workers_.load(std::memory_order_relaxed);
    s.active_routines = active_routines_.load(std::memory_order_acquire);
    s.parked_routines = parked_routines_.load(std::memory_order_relaxed);
    s.running_routines = running_routines_.load(std::memory_order_relaxed);
    s.global_queue_size = global_queue_.size();
    s.local_queue_sizes.reserve(workers_.size());
    for (const auto& w : workers_) {
        std::size_t qsz = w->local_queue_size();
        s.local_queue_sizes.push_back(qsz);
        s.total_local_queued += qsz;
    }
    s.total_spawned = total_spawned_.load(std::memory_order_relaxed);
    s.total_completed = total_completed_.load(std::memory_order_relaxed);
    s.total_steals = total_steals_.load(std::memory_order_relaxed);
    s.pending_timers = pending_timer_count();
    s.stack_pool = stack_pool_.stats();
    return s;
}

template <std::invocable F>
auto Runtime::block_on(F&& fn) -> std::invoke_result_t<F> {
    using R = std::invoke_result_t<F>;
    detail::SpinLock done_lock;
    bool is_done = false;
    detail::UniversalWaiter* waiter_ptr = nullptr;
    std::exception_ptr exc{nullptr};

    auto notify_done = [&]() noexcept {
        done_lock.lock();
        is_done = true;
        detail::UniversalWaiter* w = waiter_ptr;
        done_lock.unlock();
        if (w) {
            w->unpark();
        }
    };

    auto wait_done = [&]() noexcept {
        done_lock.lock();
        if (!is_done) {
            std::binary_semaphore fallback_sem{0};
            auto waiter = detail::UniversalWaiter::make_for_current(fallback_sem);
            waiter_ptr = &waiter;
            waiter.park_current(done_lock);
        } else {
            done_lock.unlock();
        }
    };

    if constexpr (std::is_void_v<R>) {
        spawn_raw([&, f = std::forward<F>(fn)]() mutable {
            try {
                auto task = std::move(f);
                std::invoke(task);
            } catch (...) {
                exc = std::current_exception();
            }
            if (detail::Routine* cur = detail::current_routine()) {
                cur->clear_locals();
            }
            notify_done();
        });
        wait_done();
        if (exc) {
            std::rethrow_exception(exc);
        }
    } else {
        std::optional<R> result;
        spawn_raw([&, f = std::forward<F>(fn)]() mutable {
            try {
                auto task = std::move(f);
                result.emplace(std::invoke(task));
            } catch (...) {
                exc = std::current_exception();
            }
            if (detail::Routine* cur = detail::current_routine()) {
                cur->clear_locals();
            }
            notify_done();
        });
        wait_done();
        if (exc) {
            std::rethrow_exception(exc);
        }
        return std::move(*result);
    }
}

/**
 * @namespace gorun::this_routine
 * @brief Execution control functions for the currently running green routine (analogous to `std::this_thread`).
 */
namespace this_routine {

/**
 * @brief Cooperatively yields the processor to other runnable green routines (analogous to `runtime.Gosched()` in Go).
 */
inline void yield() noexcept {
    if (detail::Worker* w = detail::current_worker()) {
        w->yield_current();
    } else {
        std::this_thread::yield();
    }
}

/**
 * @brief Suspends the current green routine for `duration` without blocking the underlying OS worker thread
 *        (analogous to `time.Sleep(d)` in Go).
 *
 * @param duration Sleep duration (e.g. `std::chrono::milliseconds(50)` or `50ms`).
 */
template <typename Rep, typename Period>
void sleep_for(std::chrono::duration<Rep, Period> duration) {
    if (duration <= std::chrono::duration<Rep, Period>::zero()) {
        return;
    }
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::duration_cast<std::chrono::steady_clock::duration>(duration);

    if (detail::Worker* w = detail::current_worker()) {
        detail::Routine* r = detail::current_routine();
        detail::SpinLock timer_park_lock;
        timer_park_lock.lock();
        Runtime* rt = &w->runtime();
        rt->schedule_timer_callback(deadline, [rt, r, &timer_park_lock]() {
            timer_park_lock.lock();
            timer_park_lock.unlock();
            rt->schedule(r);
        });
        w->park_locked(timer_park_lock);
    } else {
        std::this_thread::sleep_for(duration);
    }
}

/**
 * @brief Returns the unique 64-bit ID of the current green routine (or `0` if called from an external OS thread).
 */
[[nodiscard]] inline std::uint64_t id() noexcept {
    if (detail::Routine* r = detail::current_routine()) {
        return r->id;
    }
    return 0;
}

/**
 * @brief Returns the cooperative cancellation token (`std::stop_token`) associated with the current routine.
 */
[[nodiscard]] inline std::stop_token get_stop_token() noexcept {
    if (detail::Routine* r = detail::current_routine()) {
        return r->stop_source.get_token();
    }
    return std::stop_token{};
}

/**
 * @brief Returns the usable virtual-memory stack size (in bytes) of the current green routine, or `0` on external OS threads.
 */
[[nodiscard]] inline std::size_t stack_size() noexcept {
    if (detail::Routine* r = detail::current_routine()) {
        return r->stack.usable_bytes;
    }
    return 0;
}

} // namespace this_routine

/**
 * @brief Shorthand alias for `gorun::this_routine::yield()`.
 */
inline void yield() noexcept {
    this_routine::yield();
}

/**
 * @brief Shorthand alias for `gorun::this_routine::sleep_for(duration)`.
 */
template <typename Rep, typename Period>
void sleep_for(std::chrono::duration<Rep, Period> duration) {
    this_routine::sleep_for(duration);
}

/**
 * @class RoutineLocal
 * @brief Lock-free, zero-leak routine-local storage container isolated per green routine
 *        (safe replacement for `thread_local`).
 *
 * Values are stored directly on the owning `Routine` and automatically destroyed the instant
 * the routine finishes — preventing unbounded memory growth in long-running web servers.
 * External OS threads automatically fall back to thread-safe `thread_local` storage.
 *
 * @tparam T Default-constructible value type stored per routine.
 */
template <typename T>
class RoutineLocal {
    static std::atomic<std::uint64_t>& next_key_id() noexcept {
        static std::atomic<std::uint64_t> counter{1};
        return counter;
    }

    static void destroy_value(void* ptr) noexcept {
        delete static_cast<T*>(ptr);
    }

    const std::uint64_t key_id_;

public:
    RoutineLocal() noexcept
        : key_id_(next_key_id().fetch_add(1, std::memory_order_relaxed)) {}

    RoutineLocal(const RoutineLocal&) = delete;
    RoutineLocal& operator=(const RoutineLocal&) = delete;

    /**
     * @brief Returns a reference to the `T` instance owned by the current green routine (lock-free!)
     *        or the current external OS thread.
     */
    T& get() {
        if (detail::Routine* r = detail::current_routine()) {
            auto& slot = r->routine_locals[key_id_];
            if (!slot.ptr) {
                slot.ptr = new T{};
                slot.deleter = &destroy_value;
            }
            return *static_cast<T*>(slot.ptr);
        }

        // External OS thread fallback: strictly isolated per OS thread (zero data race!)
        thread_local std::unordered_map<std::uint64_t, detail::Routine::LocalSlot> os_thread_locals;
        auto& slot = os_thread_locals[key_id_];
        if (!slot.ptr) {
            slot.ptr = new T{};
            slot.deleter = &destroy_value;
        }
        return *static_cast<T*>(slot.ptr);
    }

    T& operator*() { return get(); }
    T* operator->() { return &get(); }
};

} // namespace gorun
