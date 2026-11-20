#pragma once

/**
 * @file sync.hpp
 * @brief Green-thread synchronization primitives with zero OS-thread blocking:
 *        `Mutex`, `CondVar`, `WaitGroup`, `Semaphore`, `Once`, and `RwMutex`.
 */

#include <cstddef>
#include <cstdint>
#include <exception>
#include <mutex>
#include <optional>
#include <semaphore>
#include <stdexcept>
#include <utility>

#include "gorun/runtime.hpp"

namespace gorun {

/**
 * @class Mutex
 * @brief Non-OS-blocking mutual exclusion lock for green routines (analogous to `sync.Mutex` in Go).
 *
 * Fully satisfies the C++ `BasicLockable` and `Lockable` named requirements, making it directly compatible
 * with standard RAII wrappers: `std::lock_guard<gorun::Mutex>`, `std::unique_lock<gorun::Mutex>`, and `std::scoped_lock`.
 *
 * When contended, `lock()` parks only the calling green routine on an intrusive stack-allocated wait queue
 * and immediately yields the underlying OS worker thread to other runnable routines.
 *
 * @code
 * gorun::Mutex mtx;
 * int shared_counter = 0;
 *
 * gorun::go([&]() {
 *     std::lock_guard lock(mtx);
 *     ++shared_counter;
 * });
 * @endcode
 */
class Mutex {
    detail::SpinLock wait_lock_;
    bool is_locked_{false};
    detail::IntrusiveQueue<detail::UniversalWaiter> waiters_;

public:
    constexpr Mutex() noexcept = default;
    Mutex(const Mutex&) = delete;
    Mutex& operator=(const Mutex&) = delete;

    /**
     * @brief Attempts to acquire the mutex without suspending the calling routine.
     * @return `true` if the lock was acquired, `false` if already held.
     */
    [[nodiscard]] bool try_lock() noexcept {
        wait_lock_.lock();
        if (!is_locked_) {
            is_locked_ = true;
            wait_lock_.unlock();
            return true;
        }
        wait_lock_.unlock();
        return false;
    }

    /**
     * @brief Acquires the mutex. If contended, parks the current green routine without blocking the OS worker thread.
     */
    void lock() noexcept {
        // Fast path: spin briefly (like Go's sync.Mutex active spinning) before parking the routine
        for (int spin = 0; spin < 32; ++spin) {
            if (try_lock()) {
                return;
            }
            detail::cpu_relax();
        }

        wait_lock_.lock();
        if (!is_locked_) {
            is_locked_ = true;
            wait_lock_.unlock();
            return;
        }

        std::binary_semaphore fallback_sem{0};
        auto waiter = detail::UniversalWaiter::make_for_current(fallback_sem);
        waiters_.push_back(&waiter);

        waiter.park_current(wait_lock_);
    }

    /**
     * @brief Releases the mutex. If routines are waiting, transfers ownership directly (Direct Handoff FIFO)
     *        to the next waiter and schedules it.
     */
    void unlock() noexcept {
        wait_lock_.lock();
        if (auto* next = waiters_.pop_front()) {
            wait_lock_.unlock();
            next->unpark();
        } else {
            is_locked_ = false;
            wait_lock_.unlock();
        }
    }
};

/**
 * @class CondVar
 * @brief Condition variable for green routines (analogous to `sync.Cond` in Go and `std::condition_variable` in C++).
 *
 * Works seamlessly with `std::unique_lock<gorun::Mutex>` without blocking OS worker threads.
 */
class CondVar {
    detail::SpinLock lock_;
    detail::IntrusiveQueue<detail::UniversalWaiter> waiters_;

public:
    constexpr CondVar() noexcept = default;
    CondVar(const CondVar&) = delete;
    CondVar& operator=(const CondVar&) = delete;

    /**
     * @brief Atomically unlocks `user_lock`, parks the current green routine, and re-locks `user_lock` upon wakeup.
     * @param user_lock Held `std::unique_lock<gorun::Mutex>`.
     */
    void wait(std::unique_lock<Mutex>& user_lock) noexcept {
        std::binary_semaphore fallback_sem{0};
        auto waiter = detail::UniversalWaiter::make_for_current(fallback_sem);

        lock_.lock();
        waiters_.push_back(&waiter);
        user_lock.unlock();

        waiter.park_current(lock_);

        user_lock.lock();
    }

    /**
     * @brief Waits until `pred()` evaluates to `true` under `user_lock`.
     * @param user_lock Held `std::unique_lock<gorun::Mutex>`.
     * @param pred Boolean predicate checked before and after suspension.
     */
    template <typename Predicate>
    void wait(std::unique_lock<Mutex>& user_lock, Predicate pred) {
        while (!pred()) {
            wait(user_lock);
        }
    }

    /**
     * @brief Wakes one waiting routine (if any).
     */
    void notify_one() noexcept {
        lock_.lock();
        auto* w = waiters_.pop_front();
        lock_.unlock();

        if (w) {
            w->unpark();
        }
    }

    /**
     * @brief Wakes all currently waiting routines.
     */
    void notify_all() noexcept {
        detail::IntrusiveQueue<detail::UniversalWaiter> to_wake;
        lock_.lock();
        to_wake = std::move(waiters_);
        lock_.unlock();

        while (auto* w = to_wake.pop_front()) {
            w->unpark();
        }
    }
};

/**
 * @class WaitGroup
 * @brief Task synchronization barrier (direct equivalent of `sync.WaitGroup` in Go).
 *
 * Allows one or more routines (or `main()`) to park via `wait()` until a counter of concurrent operations drops to zero.
 */
class WaitGroup {
    detail::SpinLock lock_;
    std::int64_t counter_{0};
    detail::IntrusiveQueue<detail::UniversalWaiter> waiters_;

public:
    constexpr WaitGroup() noexcept = default;
    WaitGroup(const WaitGroup&) = delete;
    WaitGroup& operator=(const WaitGroup&) = delete;

    /**
     * @brief Adds `delta` to the WaitGroup counter. If the counter reaches zero, all routines blocked in `wait()` are unparked.
     * @param delta Counter increment or decrement (default: `+1`).
     * @throws std::logic_error If the counter becomes negative.
     */
    void add(std::int64_t delta = 1) {
        lock_.lock();
        counter_ += delta;
        if (counter_ < 0) {
            lock_.unlock();
            throw std::logic_error("gorun::WaitGroup: negative counter");
        }
        if (counter_ > 0) {
            lock_.unlock();
            return;
        }

        auto to_wake = std::move(waiters_);
        lock_.unlock();

        while (auto* w = to_wake.pop_front()) {
            w->unpark();
        }
    }

    /**
     * @brief Decrements the WaitGroup counter by 1 (equivalent to `add(-1)`).
     */
    void done() {
        add(-1);
    }

    /**
     * @brief Parks the calling green routine (or external OS thread) until the counter reaches zero.
     */
    void wait() noexcept {
        lock_.lock();
        if (counter_ == 0) {
            lock_.unlock();
            return;
        }

        std::binary_semaphore fallback_sem{0};
        auto waiter = detail::UniversalWaiter::make_for_current(fallback_sem);
        waiters_.push_back(&waiter);
        waiter.park_current(lock_);
    }
};

/**
 * @class Semaphore
 * @brief Counting semaphore for bounding concurrency among green routines without blocking OS worker threads.
 */
class Semaphore {
    detail::SpinLock lock_;
    std::ptrdiff_t permits_;
    detail::IntrusiveQueue<detail::UniversalWaiter> waiters_;

public:
    /**
     * @brief Constructs a counting semaphore with `initial_permits` available permits.
     */
    explicit constexpr Semaphore(std::ptrdiff_t initial_permits) noexcept
        : permits_(initial_permits) {}

    Semaphore(const Semaphore&) = delete;
    Semaphore& operator=(const Semaphore&) = delete;

    /**
     * @brief Attempts to acquire a permit without parking.
     * @return `true` if a permit was acquired, `false` otherwise.
     */
    [[nodiscard]] bool try_acquire() noexcept {
        lock_.lock();
        if (permits_ > 0) {
            --permits_;
            lock_.unlock();
            return true;
        }
        lock_.unlock();
        return false;
    }

    /**
     * @brief Acquires a permit, parking the current green routine if none are currently available.
     */
    void acquire() noexcept {
        lock_.lock();
        if (permits_ > 0) {
            --permits_;
            lock_.unlock();
            return;
        }

        std::binary_semaphore fallback_sem{0};
        auto waiter = detail::UniversalWaiter::make_for_current(fallback_sem);
        waiters_.push_back(&waiter);
        waiter.park_current(lock_);
    }

    /**
     * @brief Releases `update` permits back to the semaphore, unparking waiting routines in FIFO order.
     * @param update Number of permits to release (default: `1`).
     */
    void release(std::ptrdiff_t update = 1) noexcept {
        detail::IntrusiveQueue<detail::UniversalWaiter> to_wake;
        lock_.lock();
        for (std::ptrdiff_t i = 0; i < update; ++i) {
            if (auto* w = waiters_.pop_front()) {
                to_wake.push_back(w);
            } else {
                ++permits_;
            }
        }
        lock_.unlock();

        while (auto* w = to_wake.pop_front()) {
            w->unpark();
        }
    }
};

/**
 * @class Once
 * @brief One-time initialization primitive for green routines (analogous to `sync.Once` in Go and `std::once_flag` in C++).
 *
 * Contending routines park on an internal `gorun::Mutex` instead of blocking OS worker threads.
 */
class Once {
    Mutex mutex_;
    std::atomic<bool> done_{false};

public:
    constexpr Once() noexcept = default;
    Once(const Once&) = delete;
    Once& operator=(const Once&) = delete;

    /**
     * @brief Executes `fn(args...)` exactly once across all routines calling `call()` on this instance.
     */
    template <typename Callable, typename... Args>
    void call(Callable&& fn, Args&&... args) {
        if (done_.load(std::memory_order_acquire)) {
            return;
        }
        std::lock_guard guard(mutex_);
        if (!done_.load(std::memory_order_relaxed)) {
            std::invoke(std::forward<Callable>(fn), std::forward<Args>(args)...);
            done_.store(true, std::memory_order_release);
        }
    }
};

/**
 * @brief Invokes `fn(args...)` exactly once for `once_flag` (analogous to `std::call_once`).
 */
template <typename Callable, typename... Args>
void call_once(Once& once_flag, Callable&& fn, Args&&... args) {
    once_flag.call(std::forward<Callable>(fn), std::forward<Args>(args)...);
}

/**
 * @class RwMutex
 * @brief Writer-preferring Read-Write mutex for green routines (analogous to `sync.RWMutex` in Go and `std::shared_mutex`).
 *
 * Satisfies C++ `SharedLockable`, allowing direct use with:
 * - `std::shared_lock<gorun::RwMutex>` for shared reader access;
 * - `std::unique_lock<gorun::RwMutex>` or `std::lock_guard<gorun::RwMutex>` for exclusive writer access.
 */
class RwMutex {
    detail::SpinLock lock_;
    std::size_t active_readers_{0};
    bool active_writer_{false};
    detail::IntrusiveQueue<detail::UniversalWaiter> waiting_writers_;
    detail::IntrusiveQueue<detail::UniversalWaiter> waiting_readers_;

public:
    constexpr RwMutex() noexcept = default;
    RwMutex(const RwMutex&) = delete;
    RwMutex& operator=(const RwMutex&) = delete;

    /**
     * @brief Acquires exclusive writer ownership. Parks the routine if readers or another writer hold the lock.
     */
    void lock() noexcept {
        lock_.lock();
        if (!active_writer_ && active_readers_ == 0) {
            active_writer_ = true;
            lock_.unlock();
            return;
        }

        std::binary_semaphore fallback_sem{0};
        auto waiter = detail::UniversalWaiter::make_for_current(fallback_sem);
        waiting_writers_.push_back(&waiter);
        waiter.park_current(lock_);
    }

    /**
     * @brief Attempts to acquire exclusive writer ownership without parking.
     */
    [[nodiscard]] bool try_lock() noexcept {
        lock_.lock();
        if (!active_writer_ && active_readers_ == 0) {
            active_writer_ = true;
            lock_.unlock();
            return true;
        }
        lock_.unlock();
        return false;
    }

    /**
     * @brief Releases exclusive writer ownership, waking the next waiting writer or all waiting readers.
     */
    void unlock() noexcept {
        detail::IntrusiveQueue<detail::UniversalWaiter> readers_to_wake;
        detail::UniversalWaiter* writer_to_wake = nullptr;

        lock_.lock();
        active_writer_ = false;
        if (!waiting_writers_.empty()) {
            active_writer_ = true;
            writer_to_wake = waiting_writers_.pop_front();
        } else {
            while (auto* r = waiting_readers_.pop_front()) {
                ++active_readers_;
                readers_to_wake.push_back(r);
            }
        }
        lock_.unlock();

        if (writer_to_wake) {
            writer_to_wake->unpark();
        } else {
            while (auto* r = readers_to_wake.pop_front()) {
                r->unpark();
            }
        }
    }

    /**
     * @brief Acquires shared reader ownership. Multiple readers may hold the lock simultaneously unless a writer is waiting.
     */
    void lock_shared() noexcept {
        lock_.lock();
        if (!active_writer_ && waiting_writers_.empty()) {
            ++active_readers_;
            lock_.unlock();
            return;
        }

        std::binary_semaphore fallback_sem{0};
        auto waiter = detail::UniversalWaiter::make_for_current(fallback_sem);
        waiting_readers_.push_back(&waiter);
        waiter.park_current(lock_);
    }

    /**
     * @brief Attempts to acquire shared reader ownership without parking.
     */
    [[nodiscard]] bool try_lock_shared() noexcept {
        lock_.lock();
        if (!active_writer_ && waiting_writers_.empty()) {
            ++active_readers_;
            lock_.unlock();
            return true;
        }
        lock_.unlock();
        return false;
    }

    /**
     * @brief Releases shared reader ownership. The last active reader wakes the next waiting writer (if any).
     */
    void unlock_shared() noexcept {
        detail::UniversalWaiter* writer_to_wake = nullptr;
        lock_.lock();
        --active_readers_;
        if (active_readers_ == 0 && !waiting_writers_.empty()) {
            active_writer_ = true;
            writer_to_wake = waiting_writers_.pop_front();
        }
        lock_.unlock();

        if (writer_to_wake) {
            writer_to_wake->unpark();
        }
    }
};

} // namespace gorun
