#pragma once

/**
 * @file runtime.hpp
 * @brief Core M:N work-stealing scheduler (`Runtime`, `Worker`, `Routine`), timer thread, and `this_routine` namespace.
 */

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
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

#if defined(GORUN_HAS_ASAN)
extern "C" {
void __sanitizer_start_switch_fiber(void** fake_stack_save, const void* bottom, std::size_t size);
void __sanitizer_finish_switch_fiber(void* fake_stack_save, const void** bottom_old, std::size_t* size_old);
}
#endif

#include "gorun/detail/context.hpp"
#include "gorun/detail/spinlock.hpp"
#include "gorun/detail/stack_pool.hpp"
#include "gorun/detail/work_stealing_deque.hpp"

namespace gorun {

class Runtime;
class Scope;

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
     * @brief Usable virtual-memory stack size per green routine in bytes (default: 128 KB).
     */
    std::size_t default_stack_size = 128 * 1024;

    /**
     * @brief Maximum number of idle routine stacks cached in `StackPool` for zero-syscall reuse.
     */
    std::size_t max_pooled_stacks = 1024;
};

namespace detail {

class Worker;

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
    std::uint64_t id{0};                        ///< Unique monotonically increasing routine ID
    RoutineState state{RoutineState::Ready};    ///< Current lifecycle state
    ExecutionContext ctx{};                     ///< Saved CPU registers and stack pointer (`%rsp`)
    StackAllocation stack{};                    ///< Virtual-memory stack region with hardware Guard Page
    std::function<void()> task{};               ///< User callable body
    std::stop_source stop_source{};             ///< Cooperative cancellation source for this routine
    Runtime* owner_runtime{nullptr};            ///< Owning `Runtime` instance
    Scope* implicit_scope{nullptr};             ///< Active structured concurrency `Scope` inherited by this routine

    Routine* prev{nullptr};
    Routine* next{nullptr};
#if defined(GORUN_HAS_ASAN)
    void* asan_fake_stack{nullptr};
#endif
};

inline Worker*& tls_current_worker() noexcept {
    thread_local Worker* w = nullptr;
    return w;
}

inline Routine*& tls_current_routine() noexcept {
    thread_local Routine* r = nullptr;
    return r;
}

inline Scope*& tls_implicit_scope() noexcept {
    thread_local Scope* s = nullptr;
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
 * @brief Returns the active structured concurrency `Scope` (or `TaskGroup`) for the current routine or OS thread.
 */
inline Scope* current_implicit_scope() noexcept {
    if (Routine* r = current_routine()) {
        return r->implicit_scope;
    }
    return tls_implicit_scope();
}

/**
 * @brief Sets the active structured concurrency `Scope` for the current routine or OS thread, returning the previous one.
 */
inline Scope* set_implicit_scope(Scope* s) noexcept {
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
 * Manages the pool of OS `Worker` threads, the `Global Run Queue`, the `StackPool`, and the background `Timer Thread`.
 * Can be instantiated explicitly for isolated pools (`gorun::Runtime rt(cfg);`) or accessed implicitly via
 * the lazy global singleton `gorun::default_runtime()` whenever `gorun::go(...)` is called.
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
    std::atomic<std::size_t> active_routines_{0};
    std::atomic<bool> shutdown_{false};

    struct TimerEntry {
        std::chrono::steady_clock::time_point deadline;
        detail::Routine* routine{nullptr};
        std::function<void()> callback{};

        bool operator>(const TimerEntry& other) const noexcept {
            return deadline > other.deadline;
        }
    };

    std::mutex timer_mutex_;
    std::condition_variable timer_cv_;
    std::priority_queue<TimerEntry, std::vector<TimerEntry>, std::greater<TimerEntry>> timers_;
    std::jthread timer_thread_;

    std::mutex routine_pool_mutex_;
    std::vector<std::unique_ptr<detail::Routine>> routine_pool_;

    static void routine_trampoline(void* arg) noexcept;

    detail::Routine* acquire_routine(std::function<void()> fn);
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
     * @brief Registers a timer deadline in the min-heap timer thread to wake routine `r`.
     */
    void schedule_timer(std::chrono::steady_clock::time_point deadline, detail::Routine* r);

    /**
     * @brief Registers an arbitrary callback in the min-heap timer thread at `deadline`.
     */
    void schedule_timer_callback(std::chrono::steady_clock::time_point deadline, std::function<void()> cb);

    /**
     * @brief Spawns a new green routine on this `Runtime` instance.
     * @tparam F Callable type `void()`.
     * @param fn Callable task body.
     */
    template <typename F>
    void spawn_raw(F&& fn) {
        active_routines_.fetch_add(1, std::memory_order_relaxed);
        detail::Routine* r = acquire_routine(std::forward<F>(fn));
        r->implicit_scope = detail::current_implicit_scope();
        schedule(r);
    }

    /**
     * @brief Spawns a green routine on this `Runtime` instance.
     * @tparam F Callable type `void()`.
     * @param fn Callable task body.
     */
    template <typename F>
    void go(F&& fn) {
        spawn_raw(std::forward<F>(fn));
    }

    /**
     * @brief Spawns `fn` as a main green routine on this runtime and blocks the calling OS thread until it completes.
     *
     * Any uncaught exception thrown inside `fn` is captured into a `std::exception_ptr` and rethrown to the caller.
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
    std::vector<std::unique_ptr<Routine>> local_free_routines_{}; ///< Per-worker lock-free routine cache (like Go's `p.gFree`)
    SpinLock* pending_unlock_{nullptr};
    SpinLock yield_release_lock_{};
    bool yield_requested_{false};
    bool finish_requested_{false};
    std::uint64_t tick_{0};
    std::mt19937 rng_;
#if defined(GORUN_HAS_ASAN)
    void* asan_scheduler_fake_stack_{nullptr};
    const void* asan_scheduler_bottom_{nullptr};
    std::size_t asan_scheduler_size_{0};
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

    /**
     * @brief Atomically transitions the current routine to `Parked`, switches CPU context to `scheduler_ctx_`,
     *        and releases `primitive_lock` **after** the routine's registers are safely saved on its stack.
     *
     * @note Direct C++ equivalent of Go's `gopark(unlockf, lock)` in `runtime/proc.go`.
     * Prevents both **Lost Wakeup** and **Premature Dual-Core Stack Execution**.
     *
     * @param primitive_lock Already-acquired spinlock of the synchronization primitive (`Channel`, `Mutex`, `WaitGroup`).
     */
    void park_locked(SpinLock& primitive_lock) noexcept {
        Routine* r = current_routine_;
        r->state = RoutineState::Parked;
        pending_unlock_ = &primitive_lock;
#if defined(GORUN_HAS_ASAN)
        asan_before_switch_to_scheduler(r, false);
#endif
        context_switch(&r->ctx, &scheduler_ctx_);
#if defined(GORUN_HAS_ASAN)
        current_worker()->asan_after_switch_to_routine(r);
#endif
    }

    /**
     * @brief Cooperatively yields execution back to the scheduler (`this_routine::yield()`), keeping the routine `Ready`.
     */
    void yield_current() noexcept {
        Routine* r = current_routine_;
        r->state = RoutineState::Ready;
        yield_requested_ = true;
#if defined(GORUN_HAS_ASAN)
        asan_before_switch_to_scheduler(r, false);
#endif
        context_switch(&r->ctx, &scheduler_ctx_);
#if defined(GORUN_HAS_ASAN)
        current_worker()->asan_after_switch_to_routine(r);
#endif
    }

    /**
     * @brief Marks the current routine as `Finished` and switches back to the worker scheduler loop for stack recycling.
     */
    void finish_current() noexcept {
        Routine* r = current_routine_;
        r->state = RoutineState::Finished;
        finish_requested_ = true;
#if defined(GORUN_HAS_ASAN)
        // Note: routines in our pool loop forever in routine_trampoline, so their stack stays alive for reuse!
        asan_before_switch_to_scheduler(r, false);
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
                if (runtime_.shutdown_.load(std::memory_order_acquire)) {
                    break;
                }
                if (!runtime_.global_queue_.empty() || !local_queue_.empty()) {
                    continue;
                }
                runtime_.sleeping_workers_.fetch_add(1, std::memory_order_seq_cst);
                runtime_.sleep_cv_.wait_for(lock, std::chrono::milliseconds(1));
                runtime_.sleeping_workers_.fetch_sub(1, std::memory_order_seq_cst);
                continue;
            }

            current_routine_ = r;
            tls_current_routine() = r;
            r->state = RoutineState::Running;

#if defined(GORUN_HAS_ASAN)
            asan_before_switch_to_routine(r);
#endif
            context_switch(&scheduler_ctx_, &r->ctx);
#if defined(GORUN_HAS_ASAN)
            asan_after_switch_to_scheduler();
#endif

            current_routine_ = nullptr;
            tls_current_routine() = nullptr;

            if (pending_unlock_) {
                SpinLock* lk = std::exchange(pending_unlock_, nullptr);
                lk->unlock();
            }

            if (yield_requested_) {
                yield_requested_ = false;
                // Keep on local queue if no other workers are starving, or push to local_queue_
                local_queue_.push_back(r);
            } else if (finish_requested_) {
                finish_requested_ = false;
                runtime_.release_routine(r);
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
        detail::Worker* w_entry = detail::current_worker();
        if (w_entry->pending_unlock_) {
            detail::SpinLock* lk = std::exchange(w_entry->pending_unlock_, nullptr);
            lk->unlock();
        }

        try {
            if (r->task) {
                r->task();
            }
        } catch (...) {
        }
        r->task = nullptr;
        detail::current_worker()->finish_current();
    }
}

inline detail::Routine* Runtime::acquire_routine(std::function<void()> fn) {
    std::unique_ptr<detail::Routine> r;
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

    if (!r) {
        r = std::make_unique<detail::Routine>();
        r->owner_runtime = this;
        r->stack = stack_pool_.acquire();
        r->ctx.make_routine_stack_context(r->stack, &Runtime::routine_trampoline, r.get());
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
    r->state = detail::RoutineState::Ready;
    detail::Worker* w = detail::current_worker();
    if (w && &w->runtime() == this) {
        w->local_queue_.push_run_next(r);
    } else {
        global_queue_.push_back(r);
    }

    if (sleeping_workers_.load(std::memory_order_relaxed) > 0) {
        sleep_cv_.notify_one();
    }
}

inline void Runtime::schedule_timer(std::chrono::steady_clock::time_point deadline, detail::Routine* r) {
    {
        std::lock_guard lock(timer_mutex_);
        timers_.push(TimerEntry{.deadline = deadline, .routine = r, .callback = nullptr});
    }
    timer_cv_.notify_one();
}

inline void Runtime::schedule_timer_callback(std::chrono::steady_clock::time_point deadline, std::function<void()> cb) {
    {
        std::lock_guard lock(timer_mutex_);
        timers_.push(TimerEntry{.deadline = deadline, .routine = nullptr, .callback = std::move(cb)});
    }
    timer_cv_.notify_one();
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
        if (timers_.top().deadline <= now) {
            TimerEntry entry = std::move(const_cast<TimerEntry&>(timers_.top()));
            timers_.pop();
            lock.unlock();

            if (entry.routine) {
                schedule(entry.routine);
            } else if (entry.callback) {
                entry.callback();
            }

            lock.lock();
        } else {
            auto next_deadline = timers_.top().deadline;
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

template <std::invocable F>
auto Runtime::block_on(F&& fn) -> std::invoke_result_t<F> {
    using R = std::invoke_result_t<F>;
    std::binary_semaphore done_sem{0};
    std::exception_ptr exc{nullptr};

    if constexpr (std::is_void_v<R>) {
        spawn_raw([&, f = std::forward<F>(fn)]() mutable {
            try {
                std::invoke(f);
            } catch (...) {
                exc = std::current_exception();
            }
            done_sem.release();
        });
        done_sem.acquire();
        if (exc) {
            std::rethrow_exception(exc);
        }
    } else {
        std::optional<R> result;
        spawn_raw([&, f = std::forward<F>(fn)]() mutable {
            try {
                result.emplace(std::invoke(f));
            } catch (...) {
                exc = std::current_exception();
            }
            done_sem.release();
        });
        done_sem.acquire();
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
        w->runtime().schedule_timer_callback(deadline, [w, r, &timer_park_lock]() {
            timer_park_lock.lock();
            timer_park_lock.unlock();
            w->runtime().schedule(r);
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
 * @brief Routine-local storage container isolated per green routine ID (safe replacement for `thread_local`).
 *
 * Because a green routine can migrate across different OS `Worker` threads after `park()` / `yield()`,
 * standard C++ `thread_local` variables may change mid-function. `RoutineLocal<T>` binds state to `this_routine::id()`.
 *
 * @tparam T Value type stored per routine.
 */
template <typename T>
class RoutineLocal {
    detail::SpinLock lock_;
    std::unordered_map<std::uint64_t, T> storage_;

public:
    /**
     * @brief Returns a reference to the `T` instance owned by the current green routine.
     */
    T& get() {
        std::uint64_t rid = this_routine::id();
        std::lock_guard g(lock_);
        return storage_[rid];
    }

    T& operator*() { return get(); }
    T* operator->() { return &get(); }
};

} // namespace gorun
