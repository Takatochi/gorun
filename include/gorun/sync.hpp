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
        if (!is_locked_) {
            wait_lock_.unlock();
            return;
        }
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
 */
class CondVar {
    detail::SpinLock lock_;
    detail::IntrusiveQueue<detail::UniversalWaiter> waiters_;

public:
    constexpr CondVar() noexcept = default;
    CondVar(const CondVar&) = delete;
    CondVar& operator=(const CondVar&) = delete;

    void wait(std::unique_lock<Mutex>& user_lock) noexcept {
        std::binary_semaphore fallback_sem{0};
        auto waiter = detail::UniversalWaiter::make_for_current(fallback_sem);

        lock_.lock();
        waiters_.push_back(&waiter);
        user_lock.unlock();

        waiter.park_current(lock_);

        user_lock.lock();
    }

    template <typename Predicate>
    void wait(std::unique_lock<Mutex>& user_lock, Predicate pred) {
        while (!pred()) {
            wait(user_lock);
        }
    }

    void notify_one() noexcept {
        lock_.lock();
        auto* w = waiters_.pop_front();
        lock_.unlock();

        if (w) {
            w->unpark();
        }
    }

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
 */
class WaitGroup {
    detail::SpinLock lock_;
    std::int64_t counter_{0};
    detail::IntrusiveQueue<detail::UniversalWaiter> waiters_;

public:
    constexpr WaitGroup() noexcept = default;
    WaitGroup(const WaitGroup&) = delete;
    WaitGroup& operator=(const WaitGroup&) = delete;

    void add(std::int64_t delta = 1) {
        lock_.lock();
        counter_ += delta;
        if (counter_ < 0) {
            counter_ -= delta;
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

    void done() {
        add(-1);
    }

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
    explicit constexpr Semaphore(std::ptrdiff_t initial_permits) noexcept
        : permits_(initial_permits > 0 ? initial_permits : 0) {}

    Semaphore(const Semaphore&) = delete;
    Semaphore& operator=(const Semaphore&) = delete;

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

    void release(std::ptrdiff_t update = 1) noexcept {
        if (update <= 0) {
            return;
        }
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
 * @brief One-time initialization primitive for green routines (analogous to `sync.Once` in Go).
 */
class Once {
    Mutex mutex_;
    std::atomic<bool> done_{false};

public:
    constexpr Once() noexcept = default;
    Once(const Once&) = delete;
    Once& operator=(const Once&) = delete;

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

template <typename Callable, typename... Args>
void call_once(Once& once_flag, Callable&& fn, Args&&... args) {
    once_flag.call(std::forward<Callable>(fn), std::forward<Args>(args)...);
}

/**
 * @class RwMutex
 * @brief Writer-preferring Read-Write mutex for green routines (compatible with `std::shared_lock` & `std::unique_lock`).
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

    void unlock() noexcept {
        detail::IntrusiveQueue<detail::UniversalWaiter> readers_to_wake;
        detail::UniversalWaiter* writer_to_wake = nullptr;

        lock_.lock();
        if (!active_writer_) {
            lock_.unlock();
            return;
        }
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

    void unlock_shared() noexcept {
        detail::UniversalWaiter* writer_to_wake = nullptr;
        lock_.lock();
        if (active_readers_ == 0) {
            lock_.unlock();
            return;
        }
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
