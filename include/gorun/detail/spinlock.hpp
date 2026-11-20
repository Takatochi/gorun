#pragma once

/**
 * @file spinlock.hpp
 * @brief Low-level core primitives for gorun: TTAS SpinLock and zero-allocation intrusive FIFO queue.
 *
 * These primitives are used internally by the M:N scheduler and synchronization objects
 * (`Channel<T>`, `Mutex`, `WaitGroup`, `CondVar`, `Semaphore`, `RwMutex`) to atomically
 * enqueue a routine's stack-allocated waiter node before performing a user-space `context_switch`.
 */

#include <atomic>
#include <cstdint>
#include <semaphore>
#include <utility>

#if defined(_MSC_VER) || defined(__MINGW32__) || defined(_WIN32)
#include <immintrin.h>
#elif defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#endif

namespace gorun::detail {

/**
 * @brief Emits a CPU spin-wait hint to reduce power consumption and memory bus contention.
 *
 * Executes the `PAUSE` instruction (`_mm_pause`) on x86/x86_64 and `YIELD` on ARM/AArch64.
 */
inline void cpu_relax() noexcept {
#if defined(_MSC_VER) || defined(__x86_64__) || defined(__i386__)
    _mm_pause();
#elif defined(__aarch64__) || defined(__arm__)
    __asm__ __volatile__("yield" ::: "memory");
#else
    std::atomic_signal_fence(std::memory_order_seq_cst);
#endif
}

/**
 * @class SpinLock
 * @brief Lightweight TTAS (Test-and-Test-and-Set) spinlock protecting internal wait queues.
 *
 * @note Inspired by Go's internal runtime `runtime.mutex` protecting `hchan.lock` and `semaRoot`.
 * @warning This spinlock is intended **exclusively** for ultra-short runtime critical sections
 * (a few pointer manipulations in `IntrusiveQueue`). User code inside green routines must use `gorun::Mutex`.
 */
class SpinLock {
    std::atomic<bool> locked_{false};

public:
    constexpr SpinLock() noexcept = default;
    SpinLock(const SpinLock&) = delete;
    SpinLock& operator=(const SpinLock&) = delete;

    /**
     * @brief Acquires the spinlock with `std::memory_order_acquire` semantics.
     *
     * Spins using read-only `std::memory_order_relaxed` loads while contended to avoid
     * cache-line invalidation traffic across CPU cores.
     */
    void lock() noexcept {
        for (;;) {
            if (!locked_.exchange(true, std::memory_order_acquire)) {
                return;
            }
            while (locked_.load(std::memory_order_relaxed)) {
                cpu_relax();
            }
        }
    }

    /**
     * @brief Attempts to acquire the spinlock without spinning.
     * @return `true` if the lock was acquired, `false` if already held by another thread.
     */
    [[nodiscard]] bool try_lock() noexcept {
        return !locked_.load(std::memory_order_relaxed) &&
               !locked_.exchange(true, std::memory_order_acquire);
    }

    /**
     * @brief Releases the spinlock with `std::memory_order_release` semantics.
     */
    void unlock() noexcept {
        locked_.store(false, std::memory_order_release);
    }
};

/**
 * @class IntrusiveQueue
 * @brief Doubly-linked intrusive FIFO queue with O(1) operations and zero heap allocations.
 *
 * @note Inspired by Go's `waitq` of stack-allocated/pooled `sudog` descriptors in `runtime/chan.go`.
 * Because `gorun` guarantees the **Stack Stability** invariant (routine stacks never relocate in memory),
 * waiter nodes (`Node`) are allocated directly on the sleeping routine's stack frame right before `park_locked()`.
 *
 * @tparam Node Intrusive node type providing `Node* prev` and `Node* next` members.
 */
template <typename Node>
class IntrusiveQueue {
    Node* head_{nullptr};
    Node* tail_{nullptr};

public:
    constexpr IntrusiveQueue() noexcept = default;

    IntrusiveQueue(const IntrusiveQueue&) = delete;
    IntrusiveQueue& operator=(const IntrusiveQueue&) = delete;

    /**
     * @brief Moves all queued nodes in O(1) time (used for batch wakeups in `notify_all()` and `Channel::close()`).
     */
    IntrusiveQueue(IntrusiveQueue&& other) noexcept
        : head_(std::exchange(other.head_, nullptr))
        , tail_(std::exchange(other.tail_, nullptr)) {}

    IntrusiveQueue& operator=(IntrusiveQueue&& other) noexcept {
        if (this != &other) {
            head_ = std::exchange(other.head_, nullptr);
            tail_ = std::exchange(other.tail_, nullptr);
        }
        return *this;
    }

    /**
     * @brief Checks whether the queue contains no elements.
     * @return `true` if the queue is empty.
     */
    [[nodiscard]] bool empty() const noexcept {
        return head_ == nullptr;
    }

    /**
     * @brief Appends a waiter node to the tail of the queue in O(1) time.
     * @param node Pointer to a stack-allocated waiter node.
     */
    void push_back(Node* node) noexcept {
        node->next = nullptr;
        node->prev = tail_;
        if (tail_) {
            tail_->next = node;
        } else {
            head_ = node;
        }
        tail_ = node;
    }

    /**
     * @brief Removes and returns the oldest node from the head of the queue (FIFO) in O(1) time.
     * @return Pointer to the dequeued node, or `nullptr` if the queue is empty.
     */
    Node* pop_front() noexcept {
        if (!head_) {
            return nullptr;
        }
        Node* node = head_;
        head_ = head_->next;
        if (head_) {
            head_->prev = nullptr;
        } else {
            tail_ = nullptr;
        }
        node->next = nullptr;
        node->prev = nullptr;
        return node;
    }

    /**
     * @brief Unlinks an arbitrary node from the queue in O(1) time.
     * @param node Pointer to the node to remove.
     */
    void remove(Node* node) noexcept {
        if (node->prev) {
            node->prev->next = node->next;
        } else if (head_ == node) {
            head_ = node->next;
        }

        if (node->next) {
            node->next->prev = node->prev;
        } else if (tail_ == node) {
            tail_ = node->prev;
        }

        node->prev = nullptr;
        node->next = nullptr;
    }
};

} // namespace gorun::detail
