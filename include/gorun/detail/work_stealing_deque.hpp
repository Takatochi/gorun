#pragma once

/**
 * @file work_stealing_deque.hpp
 * @brief Per-worker double-ended run queue supporting LIFO local execution and FIFO work-stealing.
 */

#include <atomic>
#include <cstddef>
#include <mutex>
#include <optional>
#include <vector>
#include "gorun/detail/spinlock.hpp"

namespace gorun::detail {

/**
 * @class WorkStealingDeque
 * @brief Double-ended circular run queue for an M:N worker thread.
 *
 * @note Inspired by Go's per-processor (`P`) run queue (`runq` / `runnext` in `runtime/proc.go`).
 *
 * Combines cache locality with load balancing:
 * - **Owner Worker** pushes and pops from the tail (`push_back` / `pop_back`) in **LIFO** order so that
 *   newly spawned or freshly unparked routines execute immediately while their working set is hot in L1/L2 cache.
 * - **Thief Workers** steal half of the victim queue from the head (`steal_half_into`) in **FIFO** order,
 *   acquiring the oldest (and typically largest/most independent) tasks in a single batched lock acquisition.
 *
 * @tparam T Element type stored in the deque (typically `Routine*`).
 */
template <typename T>
class WorkStealingDeque {
    mutable SpinLock lock_;
    T run_next_{nullptr}; ///< 1-element hot LIFO slot (Go's `p.runnext`), protected by lock_ so thieves can also steal it!
    std::vector<T> buffer_;
    std::size_t head_{0};
    std::size_t tail_{0};
    std::size_t count_{0};

    void grow_if_needed() {
        if (count_ < buffer_.size()) {
            return;
        }
        const std::size_t old_cap = buffer_.size();
        const std::size_t new_cap = old_cap == 0 ? 256 : old_cap * 2;
        std::vector<T> next_buf(new_cap);
        for (std::size_t i = 0; i < count_; ++i) {
            next_buf[i] = buffer_[(head_ + i) % old_cap];
        }
        buffer_ = std::move(next_buf);
        head_ = 0;
        tail_ = count_;
    }

public:
    /**
     * @brief Constructs a `WorkStealingDeque` with the given initial capacity.
     * @param initial_capacity Initial circular buffer size (default: 256 elements).
     */
    explicit WorkStealingDeque(std::size_t initial_capacity = 256)
        : buffer_(initial_capacity) {}

    /**
     * @brief Places `item` into the hot `run_next_` slot (like Go's `runqput(..., next=true)`).
     * If `run_next_` was already occupied, the displaced routine is pushed to the tail of the circular buffer.
     * @return `true` if a previous routine was displaced (meaning >=2 tasks are queued locally), `false` otherwise.
     */
    bool push_run_next(T item) {
        std::lock_guard guard(lock_);
        if (!run_next_) {
            run_next_ = item;
            return count_ > 0;
        }
        T displaced = std::exchange(run_next_, item);
        grow_if_needed();
        buffer_[tail_] = displaced;
        tail_ = (tail_ + 1) % buffer_.size();
        ++count_;
        return true;
    }

    /**
     * @brief Pushes a routine to the tail of the deque.
     * @param item Routine pointer to enqueue.
     */
    void push_back(T item) {
        std::lock_guard guard(lock_);
        grow_if_needed();
        buffer_[tail_] = item;
        tail_ = (tail_ + 1) % buffer_.size();
        ++count_;
    }

    /**
     * @brief Pushes a yielded routine to the head of the deque (FIFO fair placement) so other queued
     *        routines run before it on LIFO `pop_back()`, preventing self-starvation on `yield()`.
     * @param item Routine pointer to enqueue.
     */
    void push_front(T item) {
        std::lock_guard guard(lock_);
        grow_if_needed();
        head_ = (head_ + buffer_.size() - 1) % buffer_.size();
        buffer_[head_] = item;
        ++count_;
    }

    /**
     * @brief Pops the most recently pushed routine: checks `run_next_` first, then the tail of the circular buffer (LIFO).
     * @return Routine pointer, or `nullptr` if empty.
     */
    T pop_back() noexcept {
        std::lock_guard guard(lock_);
        if (run_next_) {
            return std::exchange(run_next_, nullptr);
        }
        if (count_ == 0) {
            return nullptr;
        }
        tail_ = (tail_ + buffer_.size() - 1) % buffer_.size();
        T item = buffer_[tail_];
        --count_;
        return item;
    }

    /**
     * @brief Pops the oldest routine from the head (FIFO — used by the Global Run Queue).
     * @return Routine pointer, or `nullptr` if empty.
     */
    T pop_front() noexcept {
        std::lock_guard guard(lock_);
        if (count_ > 0) {
            T item = buffer_[head_];
            head_ = (head_ + 1) % buffer_.size();
            --count_;
            return item;
        }
        if (run_next_) {
            return std::exchange(run_next_, nullptr);
        }
        return nullptr;
    }

    /**
     * @brief Steals half of the queued tasks from the head of this deque (FIFO), including `run_next_` if the circular
     *        buffer is empty, and transfers them into `dst_deque`.
     *
     * @param dst_deque Local deque of the stealing worker thread.
     * @return The first stolen routine (to be executed immediately by the thief), or `nullptr` if this deque was empty.
     */
    T steal_half_into(WorkStealingDeque<T>& dst_deque) {
        std::vector<T> stolen;
        {
            std::lock_guard guard(lock_);
            if (count_ == 0) {
                if (run_next_) {
                    return std::exchange(run_next_, nullptr);
                }
                return nullptr;
            }
            std::size_t to_steal = (count_ + 1) / 2;
            stolen.reserve(to_steal);
            for (std::size_t i = 0; i < to_steal; ++i) {
                stolen.push_back(buffer_[head_]);
                head_ = (head_ + 1) % buffer_.size();
                --count_;
            }
        }

        T first = stolen.front();
        if (stolen.size() > 1) {
            for (std::size_t i = 1; i < stolen.size(); ++i) {
                dst_deque.push_back(stolen[i]);
            }
        }
        return first;
    }

    /**
     * @brief Returns the current number of elements in the deque (including `run_next_`).
     */
    [[nodiscard]] std::size_t size() const noexcept {
        std::lock_guard guard(lock_);
        return count_ + (run_next_ != nullptr ? 1 : 0);
    }

    /**
     * @brief Checks whether both `run_next_` and the circular buffer are empty.
     */
    [[nodiscard]] bool empty() const noexcept {
        std::lock_guard guard(lock_);
        return count_ == 0 && run_next_ == nullptr;
    }
};

} // namespace gorun::detail
