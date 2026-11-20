#pragma once

/**
 * @file channel.hpp
 * @brief Typed message-passing channels `gorun::Channel<T>` (buffered and unbuffered rendezvous)
 *        and the zero-busy-spin parked `gorun::select(...)` multiplexer.
 */

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <semaphore>
#include <tuple>
#include <utility>

#include "gorun/runtime.hpp"

namespace gorun {

namespace detail {

/**
 * @struct ChannelWaiter
 * @brief Intrusive stack-allocated waiter node for a blocked sender or receiver on a `Channel<T>` (Go's `sudog`).
 *
 * Holds a direct pointer (`data_slot`) to the `T` or `std::optional<T>` variable on the parked routine's stack frame,
 * enabling zero-allocation **Direct Stack-to-Stack Handoff** via `std::move`.
 * Also supports true parked `gorun::select(...)` via `select_winner` and `select_park_lock`.
 */
template <typename T>
struct ChannelWaiter {
    UniversalWaiter waiter{};
    void* data_slot{nullptr};
    bool woken_by_close{false};
    bool queued{false};

    // Multi-channel select() coordination
    std::atomic<int>* select_winner{nullptr};
    SpinLock* select_park_lock{nullptr};
    int select_case_index{0};

    ChannelWaiter* prev{nullptr};
    ChannelWaiter* next{nullptr};

    bool try_claim_for_select() noexcept {
        if (!select_winner) {
            return true;
        }
        int expected = -1;
        return select_winner->compare_exchange_strong(expected, select_case_index, std::memory_order_acq_rel);
    }

    // Wake helper that respects select_park_lock synchronization (guarantees context_switch finished before unpark!)
    void wake_after_unlock() noexcept {
        UniversalWaiter w = waiter;
        SpinLock* pl = select_park_lock;
        if (pl) {
            pl->lock();
            pl->unlock();
        }
        w.unpark();
    }
};

struct SelectWakeTarget {
    UniversalWaiter waiter{};
    SpinLock* select_park_lock{nullptr};

    void wake() noexcept {
        if (select_park_lock) {
            select_park_lock->lock();
            select_park_lock->unlock();
        }
        waiter.unpark();
    }
};

} // namespace detail

template <typename T, typename Callback>
struct RecvCase;

template <typename T, typename Callback>
struct SendCase;

/**
 * @class Channel
 * @brief Typed MPMC (Multi-Producer Multi-Consumer) channel modeled directly on Go's `chan T` (`runtime.hchan`).
 *
 * Exception-safe (`std::unique_lock` around all move constructions), zero-allocation on `send`/`recv`/`close`,
 * and supports true parked `gorun::select(...)` with zero busy-spinning.
 *
 * @tparam T Payload type transmitted across the channel (supports move-only types such as `std::unique_ptr<U>`).
 */
template <typename T>
class Channel {
private:
    template <typename U, typename Cb>
    friend struct RecvCase;
    template <typename U, typename Cb>
    friend struct SendCase;

    detail::SpinLock lock_;
    const std::size_t capacity_;
    std::unique_ptr<std::optional<T>[]> buffer_;
    std::size_t head_{0};
    std::size_t tail_{0};
    std::size_t count_{0};

    detail::IntrusiveQueue<detail::ChannelWaiter<T>> senders_;
    detail::IntrusiveQueue<detail::ChannelWaiter<T>> receivers_;
    bool closed_{false};

    detail::ChannelWaiter<T>* pop_valid_receiver() noexcept {
        while (auto* r = receivers_.pop_front()) {
            r->queued = false;
            if (r->try_claim_for_select()) {
                return r;
            }
        }
        return nullptr;
    }

    detail::ChannelWaiter<T>* pop_valid_sender() noexcept {
        while (auto* s = senders_.pop_front()) {
            s->queued = false;
            if (s->try_claim_for_select()) {
                return s;
            }
        }
        return nullptr;
    }

    // Internal lock access for deadlock-free sorted multi-channel select() locking
    detail::SpinLock* internal_lock_ptr() noexcept { return &lock_; }

    // Called while lock_ is ALREADY HELD by select()
    bool try_recv_locked(std::optional<T>& out_val, bool& is_closed_empty_out,
                         std::optional<detail::SelectWakeTarget>& wake_out) {
        is_closed_empty_out = false;
        if (count_ > 0) {
            out_val = std::move(buffer_[head_]);
            buffer_[head_].reset();
            head_ = (head_ + 1) % capacity_;
            --count_;

            if (auto* sender = pop_valid_sender()) {
                T* src = static_cast<T*>(sender->data_slot);
                try {
                    buffer_[tail_].emplace(std::move(*src));
                } catch (...) {
                    sender->queued = true;
                    senders_.push_back(sender);
                    throw;
                }
                tail_ = (tail_ + 1) % capacity_;
                ++count_;
                wake_out = detail::SelectWakeTarget{sender->waiter, sender->select_park_lock};
            }
            return true;
        }

        if (auto* sender = pop_valid_sender()) {
            T* src = static_cast<T*>(sender->data_slot);
            try {
                out_val.emplace(std::move(*src));
            } catch (...) {
                sender->queued = true;
                senders_.push_back(sender);
                throw;
            }
            wake_out = detail::SelectWakeTarget{sender->waiter, sender->select_park_lock};
            return true;
        }

        if (closed_) {
            is_closed_empty_out = true;
            out_val = std::nullopt;
            return true;
        }

        return false;
    }

    // Called while lock_ is ALREADY HELD by select()
    bool try_send_locked(T& value, bool& is_closed_out,
                         std::optional<detail::SelectWakeTarget>& wake_out) {
        if (closed_) {
            is_closed_out = true;
            return false;
        }
        is_closed_out = false;

        if (auto* receiver = pop_valid_receiver()) {
            auto* dst = static_cast<std::optional<T>*>(receiver->data_slot);
            try {
                dst->emplace(std::move(value));
            } catch (...) {
                receiver->queued = true;
                receivers_.push_back(receiver);
                throw;
            }
            wake_out = detail::SelectWakeTarget{receiver->waiter, receiver->select_park_lock};
            return true;
        }

        if (count_ < capacity_) {
            buffer_[tail_].emplace(std::move(value));
            tail_ = (tail_ + 1) % capacity_;
            ++count_;
            return true;
        }

        return false;
    }

    void enqueue_receiver_locked(detail::ChannelWaiter<T>* node) noexcept {
        node->queued = true;
        receivers_.push_back(node);
    }

    void enqueue_sender_locked(detail::ChannelWaiter<T>* node) noexcept {
        node->queued = true;
        senders_.push_back(node);
    }

    void remove_receiver(detail::ChannelWaiter<T>* node) noexcept {
        std::lock_guard guard(lock_);
        if (node->queued) {
            receivers_.remove(node);
            node->queued = false;
        }
    }

    void remove_sender(detail::ChannelWaiter<T>* node) noexcept {
        std::lock_guard guard(lock_);
        if (node->queued) {
            senders_.remove(node);
            node->queued = false;
        }
    }

public:
    /**
     * @brief Constructs a channel with the given buffer capacity.
     * @param capacity Buffer capacity (`0` for an unbuffered synchronous rendezvous channel).
     */
    explicit Channel(std::size_t capacity = 0)
        : capacity_(capacity)
        , buffer_(capacity > 0 ? std::make_unique<std::optional<T>[]>(capacity) : nullptr) {}

    /**
     * @brief Closes the channel and wakes any remaining parked readers or writers upon destruction.
     */
    ~Channel() {
        close();
    }

    Channel(const Channel&) = delete;
    Channel& operator=(const Channel&) = delete;

    /**
     * @brief Non-blocking attempt to send a value into the channel.
     * @param value Reference to the value to send (moved only if the send succeeds).
     * @param is_closed_out Set to `true` if the channel is already closed.
     * @return `true` if the value was delivered to a waiting receiver or placed in the buffer.
     */
    bool try_send(T& value, bool& is_closed_out) {
        std::optional<detail::SelectWakeTarget> to_wake;
        bool ok = false;
        {
            std::lock_guard guard(lock_);
            ok = try_send_locked(value, is_closed_out, to_wake);
        }
        if (to_wake) {
            to_wake->wake();
        }
        return ok;
    }

    /**
     * @brief Non-blocking attempt to receive a value from the channel.
     * @param out_val Destination `std::optional<T>` receiving the value.
     * @param is_closed_empty_out Set to `true` if the channel is closed and drained.
     * @return `true` if a value was received or the channel is closed and empty.
     */
    bool try_recv(std::optional<T>& out_val, bool& is_closed_empty_out) {
        std::optional<detail::SelectWakeTarget> to_wake;
        bool ok = false;
        {
            std::lock_guard guard(lock_);
            ok = try_recv_locked(out_val, is_closed_empty_out, to_wake);
        }
        if (to_wake) {
            to_wake->wake();
        }
        return ok;
    }

    /**
     * @brief Sends `value` into the channel (`ch <- value` in Go).
     *
     * Exception-safe: if moving `T` throws, the internal spinlock is released via RAII and waiting
     * receivers are never leaked.
     *
     * @param value Value to transmit (moved into the receiver or buffer).
     * @return `true` if delivered; `false` if the channel is closed.
     */
    bool send(T value) {
        std::unique_lock<detail::SpinLock> guard(lock_);

        std::optional<detail::SelectWakeTarget> to_wake;
        bool is_closed = false;
        if (try_send_locked(value, is_closed, to_wake)) {
            guard.unlock();
            if (to_wake) {
                to_wake->wake();
            }
            return true;
        }
        if (is_closed) {
            return false;
        }

        std::binary_semaphore fallback_sem{0};
        detail::ChannelWaiter<T> node{
            .waiter = detail::UniversalWaiter::make_for_current(fallback_sem),
            .data_slot = &value,
            .woken_by_close = false,
            .queued = true
        };

        senders_.push_back(&node);
        guard.release(); // Ownership of lock_ transfers atomically to park_current()!
        node.waiter.park_current(lock_);

        return !node.woken_by_close;
    }

    /**
     * @brief Receives the next value from the channel (`v, ok := <-ch` in Go).
     *
     * @return `std::optional<T>` containing the received item, or `std::nullopt` once the channel is closed and drained.
     */
    std::optional<T> recv() {
        std::unique_lock<detail::SpinLock> guard(lock_);

        std::optional<T> result;
        std::optional<detail::SelectWakeTarget> to_wake;
        bool closed_empty = false;
        if (try_recv_locked(result, closed_empty, to_wake)) {
            guard.unlock();
            if (to_wake) {
                to_wake->wake();
            }
            return result;
        }

        std::optional<T> result_slot;
        std::binary_semaphore fallback_sem{0};
        detail::ChannelWaiter<T> node{
            .waiter = detail::UniversalWaiter::make_for_current(fallback_sem),
            .data_slot = &result_slot,
            .woken_by_close = false,
            .queued = true
        };

        receivers_.push_back(&node);
        guard.release(); // Ownership of lock_ transfers atomically to park_current()!
        node.waiter.park_current(lock_);

        if (node.woken_by_close) {
            return std::nullopt;
        }
        return result_slot;
    }

    /**
     * @brief Closes the channel (`close(ch)` in Go) and wakes all parked readers and writers.
     *
     * Zero heap allocations (`noexcept`-safe: cannot trigger `std::terminate()` on OOM).
     */
    void close() noexcept {
        detail::IntrusiveQueue<detail::ChannelWaiter<T>> to_wake_receivers;
        detail::IntrusiveQueue<detail::ChannelWaiter<T>> to_wake_senders;

        {
            std::lock_guard guard(lock_);
            if (closed_) {
                return;
            }
            closed_ = true;

            while (auto* r = receivers_.pop_front()) {
                r->queued = false;
                if (r->try_claim_for_select()) {
                    r->woken_by_close = true;
                    to_wake_receivers.push_back(r);
                }
            }
            while (auto* s = senders_.pop_front()) {
                s->queued = false;
                if (s->try_claim_for_select()) {
                    s->woken_by_close = true;
                    to_wake_senders.push_back(s);
                }
            }
        }

        while (auto* r = to_wake_receivers.pop_front()) {
            r->wake_after_unlock();
        }
        while (auto* s = to_wake_senders.pop_front()) {
            s->wake_after_unlock();
        }
    }

    [[nodiscard]] bool is_closed() noexcept {
        std::lock_guard g(lock_);
        return closed_;
    }

    [[nodiscard]] std::size_t size() noexcept {
        std::lock_guard g(lock_);
        return count_;
    }

    [[nodiscard]] std::size_t capacity() const noexcept {
        return capacity_;
    }

    template <typename Callback>
    auto on_recv(Callback&& cb);

    template <typename Callback>
    auto on_send(T val, Callback&& cb);
};

template <typename T, typename Callback>
struct RecvCase {
    static constexpr bool kHandlesClose = std::is_invocable_v<Callback, std::optional<T>>;

    Channel<T>* channel;
    Callback callback;
    std::optional<T> result_slot{};
    detail::ChannelWaiter<T> waiter_node{};
    bool closed_skipped{false};

    detail::SpinLock* lock_ptr() noexcept { return channel->internal_lock_ptr(); }

    // Non-blocking fast path (locks channel itself)
    bool try_execute() {
        std::optional<T> val;
        bool closed_empty = false;
        if (channel->try_recv(val, closed_empty)) {
            if constexpr (kHandlesClose) {
                callback(std::move(val));
                return true;
            } else {
                if (!closed_empty && val.has_value()) {
                    callback(std::move(*val));
                    return true;
                }
                closed_skipped = true;
                return false;
            }
        }
        return false;
    }

    // Locked check during select() Phase 2 (channel lock is ALREADY held!)
    bool try_claim_locked(std::optional<detail::SelectWakeTarget>& wake_out) {
        bool closed_empty = false;
        if (channel->try_recv_locked(result_slot, closed_empty, wake_out)) {
            if constexpr (kHandlesClose) {
                return true;
            } else {
                if (!closed_empty && result_slot.has_value()) {
                    return true;
                }
                closed_skipped = true;
                return false;
            }
        }
        return false;
    }

    void enqueue_locked(detail::UniversalWaiter w, std::atomic<int>* winner,
                        detail::SpinLock* park_lock, int case_idx) noexcept {
        if (closed_skipped) return;
        waiter_node.waiter = w;
        waiter_node.data_slot = &result_slot;
        waiter_node.woken_by_close = false;
        waiter_node.select_winner = winner;
        waiter_node.select_park_lock = park_lock;
        waiter_node.select_case_index = case_idx;
        channel->enqueue_receiver_locked(&waiter_node);
    }

    void unlink() noexcept {
        channel->remove_receiver(&waiter_node);
    }

    void invoke_winner() {
        if constexpr (kHandlesClose) {
            if (waiter_node.woken_by_close) {
                callback(std::nullopt);
            } else {
                callback(std::move(result_slot));
            }
        } else {
            if (!waiter_node.woken_by_close && result_slot.has_value()) {
                callback(std::move(*result_slot));
            }
        }
    }
};

template <typename T, typename Callback>
struct SendCase {
    static constexpr bool kHandlesClose = std::is_invocable_v<Callback, bool>;

    Channel<T>* channel;
    T value;
    Callback callback;
    detail::ChannelWaiter<T> waiter_node{};
    bool closed_skipped{false};
    bool sent_ok{false};

    detail::SpinLock* lock_ptr() noexcept { return channel->internal_lock_ptr(); }

    bool try_execute() {
        bool is_closed = false;
        if (channel->try_send(value, is_closed)) {
            if constexpr (kHandlesClose) {
                callback(true);
            } else {
                callback();
            }
            return true;
        }
        if (is_closed) {
            if constexpr (kHandlesClose) {
                callback(false);
                return true;
            }
            closed_skipped = true;
        }
        return false;
    }

    bool try_claim_locked(std::optional<detail::SelectWakeTarget>& wake_out) {
        bool is_closed = false;
        if (channel->try_send_locked(value, is_closed, wake_out)) {
            sent_ok = true;
            return true;
        }
        if (is_closed) {
            if constexpr (kHandlesClose) {
                sent_ok = false;
                return true;
            }
            closed_skipped = true;
        }
        return false;
    }

    void enqueue_locked(detail::UniversalWaiter w, std::atomic<int>* winner,
                        detail::SpinLock* park_lock, int case_idx) noexcept {
        if (closed_skipped) return;
        waiter_node.waiter = w;
        waiter_node.data_slot = &value;
        waiter_node.woken_by_close = false;
        waiter_node.select_winner = winner;
        waiter_node.select_park_lock = park_lock;
        waiter_node.select_case_index = case_idx;
        channel->enqueue_sender_locked(&waiter_node);
    }

    void unlink() noexcept {
        channel->remove_sender(&waiter_node);
    }

    void invoke_winner() {
        if constexpr (kHandlesClose) {
            callback(sent_ok || !waiter_node.woken_by_close);
        } else {
            if (sent_ok || !waiter_node.woken_by_close) {
                callback();
            }
        }
    }
};

template <typename Callback>
struct DefaultCase {
    Callback callback;
};

template <typename Rep, typename Period, typename Callback>
struct TimeoutCase {
    std::chrono::duration<Rep, Period> duration;
    Callback callback;
};

template <typename T>
template <typename Callback>
auto Channel<T>::on_recv(Callback&& cb) {
    return RecvCase<T, std::decay_t<Callback>>{this, std::forward<Callback>(cb)};
}

template <typename T>
template <typename Callback>
auto Channel<T>::on_send(T val, Callback&& cb) {
    return SendCase<T, std::decay_t<Callback>>{this, std::move(val), std::forward<Callback>(cb)};
}

template <typename Callback>
auto on_default(Callback&& cb) {
    return DefaultCase<std::decay_t<Callback>>{std::forward<Callback>(cb)};
}

template <typename Rep, typename Period, typename Callback>
auto on_timeout(std::chrono::duration<Rep, Period> d, Callback&& cb) {
    return TimeoutCase<Rep, Period, std::decay_t<Callback>>{d, std::forward<Callback>(cb)};
}

namespace detail {

template <typename Case>
struct IsDefaultCase : std::false_type {};
template <typename Cb>
struct IsDefaultCase<DefaultCase<Cb>> : std::true_type {};

template <typename Case>
struct IsTimeoutCase : std::false_type {};
template <typename R, typename P, typename Cb>
struct IsTimeoutCase<TimeoutCase<R, P, Cb>> : std::true_type {};

inline std::uint64_t fast_select_rand() noexcept {
    thread_local std::uint64_t state = 0x9E3779B97F4A7C15ULL ^
        reinterpret_cast<std::uintptr_t>(&state);
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    return state;
}

} // namespace detail

/**
 * @brief Zero-busy-spin multi-channel `select` statement modeled on Go's `select { ... }` (`runtime.selectgo`).
 *
 * 1. **Phase 1 (Fast Poll):** Polls all channel clauses in pseudo-random order. If any clause is ready, executes it immediately.
 *    If none is ready and `on_default(...)` is present, executes the default clause immediately.
 * 2. **Phase 2 (True Parked Wait — 0% CPU):** Locks all participating channels in sorted pointer-address order
 *    (deadlock-free!), re-checks readiness, registers stack-allocated `ChannelWaiter` nodes on every channel and a
 *    cancellable timer for `on_timeout(...)`, and parks the routine cleanly via `park_current()`.
 *    Upon wakeup, immediately cancels the timer and unlinks all losing waiters before invoking the winning callback.
 */
template <typename... Cases>
void select(Cases&&... cases) {
    auto tuple_cases = std::make_tuple(std::forward<Cases>(cases)...);
    constexpr std::size_t N = sizeof...(Cases);
    if constexpr (N == 0) {
        return;
    }

    // Build pseudo-random poll order (Fisher-Yates with fast xorshift64)
    std::array<std::size_t, N> order{};
    for (std::size_t i = 0; i < N; ++i) order[i] = i;
    for (std::size_t i = N - 1; i > 0; --i) {
        std::size_t j = static_cast<std::size_t>(detail::fast_select_rand() % (i + 1));
        std::swap(order[i], order[j]);
    }

    // ---- PHASE 1: Fast Non-Blocking Poll ----
    for (std::size_t idx : order) {
        bool executed = false;
        auto try_one = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            (([&] {
                if (!executed && idx == Is) {
                    auto& c = std::get<Is>(tuple_cases);
                    using CType = std::decay_t<decltype(c)>;
                    if constexpr (!detail::IsDefaultCase<CType>::value &&
                                  !detail::IsTimeoutCase<CType>::value) {
                        if (c.try_execute()) {
                            executed = true;
                        }
                    }
                }
            }()), ...);
        };
        try_one(std::make_index_sequence<N>{});
        if (executed) return;
    }

    // Check DefaultCase or zero/negative TimeoutCase
    {
        bool finished = false;
        auto check_immediate = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            (([&] {
                if (finished) return;
                auto& c = std::get<Is>(tuple_cases);
                using CType = std::decay_t<decltype(c)>;
                if constexpr (detail::IsDefaultCase<CType>::value) {
                    c.callback();
                    finished = true;
                } else if constexpr (detail::IsTimeoutCase<CType>::value) {
                    if (c.duration <= decltype(c.duration)::zero()) {
                        c.callback();
                        finished = true;
                    }
                }
            }()), ...);
        };
        check_immediate(std::make_index_sequence<N>{});
        if (finished) return;
    }

    // ---- PHASE 2: True Parked Multi-Channel Wait (Zero Busy-Spin!) ----
    // 2.1 Collect and sort unique channel SpinLock* addresses to guarantee deadlock-free lock ordering
    std::array<detail::SpinLock*, N> lock_ptrs{};
    std::size_t lock_count = 0;
    {
        auto collect_locks = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            (([&] {
                auto& c = std::get<Is>(tuple_cases);
                using CType = std::decay_t<decltype(c)>;
                if constexpr (!detail::IsDefaultCase<CType>::value &&
                              !detail::IsTimeoutCase<CType>::value) {
                    if (!c.closed_skipped) {
                        lock_ptrs[lock_count++] = c.lock_ptr();
                    }
                }
            }()), ...);
        };
        collect_locks(std::make_index_sequence<N>{});
    }
    // Insertion sort + deduplicate SpinLock* pointers (avoids std::sort introsort array-bounds warnings on small N)
    for (std::size_t i = 1; i < lock_count; ++i) {
        detail::SpinLock* key = lock_ptrs[i];
        std::size_t j = i;
        while (j > 0 && std::less<detail::SpinLock*>{}(key, lock_ptrs[j - 1])) {
            lock_ptrs[j] = lock_ptrs[j - 1];
            --j;
        }
        lock_ptrs[j] = key;
    }
    if (lock_count > 1) {
        std::size_t write_idx = 1;
        for (std::size_t read_idx = 1; read_idx < lock_count; ++read_idx) {
            if (lock_ptrs[read_idx] != lock_ptrs[write_idx - 1]) {
                lock_ptrs[write_idx++] = lock_ptrs[read_idx];
            }
        }
        lock_count = write_idx;
    }

    auto lock_all = [&]() noexcept {
        for (std::size_t i = 0; i < lock_count; ++i) lock_ptrs[i]->lock();
    };
    auto unlock_all = [&]() noexcept {
        for (std::size_t i = lock_count; i > 0; --i) lock_ptrs[i - 1]->unlock();
    };

    std::atomic<int> winner{-1};
    detail::SpinLock park_lock;
    std::binary_semaphore fallback_sem{0};
    auto waiter = detail::UniversalWaiter::make_for_current(fallback_sem);

    // Track timer for TimeoutCase (if any)
    Runtime* timer_rt = (detail::current_worker() != nullptr)
                            ? &detail::current_worker()->runtime()
                            : &default_runtime();
    std::array<std::uint64_t, N> timer_ids{};
    std::size_t timer_count = 0;
    bool has_any_waiter = false;

    int immediate_win_idx = -1;
    std::optional<detail::SelectWakeTarget> immediate_wake;

    // Acquire park_lock BEFORE unlocking channel locks so no concurrent unpark can race with context_switch!
    park_lock.lock();
    lock_all();

    // 2.2 Re-check readiness under all channel locks in randomized order
    for (std::size_t idx : order) {
        auto check_locked = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            (([&] {
                if (immediate_win_idx != -1 || idx != Is) return;
                auto& c = std::get<Is>(tuple_cases);
                using CType = std::decay_t<decltype(c)>;
                if constexpr (!detail::IsDefaultCase<CType>::value &&
                              !detail::IsTimeoutCase<CType>::value) {
                    if (!c.closed_skipped && c.try_claim_locked(immediate_wake)) {
                        immediate_win_idx = static_cast<int>(Is);
                    }
                }
            }()), ...);
        };
        check_locked(std::make_index_sequence<N>{});
        if (immediate_win_idx != -1) break;
    }

    if (immediate_win_idx != -1) {
        unlock_all();
        park_lock.unlock();
        if (immediate_wake) {
            immediate_wake->wake();
        }
        auto invoke_imm = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            (([&] {
                if (immediate_win_idx == static_cast<int>(Is)) {
                    auto& c = std::get<Is>(tuple_cases);
                    using CType = std::decay_t<decltype(c)>;
                    if constexpr (!detail::IsDefaultCase<CType>::value &&
                                  !detail::IsTimeoutCase<CType>::value) {
                        c.invoke_winner();
                    }
                }
            }()), ...);
        };
        invoke_imm(std::make_index_sequence<N>{});
        return;
    }

    // 2.3 Enqueue stack-allocated waiters into all open channels while holding all channel locks
    {
        auto enqueue_all = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            (([&] {
                auto& c = std::get<Is>(tuple_cases);
                using CType = std::decay_t<decltype(c)>;
                if constexpr (!detail::IsDefaultCase<CType>::value &&
                              !detail::IsTimeoutCase<CType>::value) {
                    if (!c.closed_skipped) {
                        c.enqueue_locked(waiter, &winner, &park_lock, static_cast<int>(Is));
                        has_any_waiter = true;
                    }
                }
            }()), ...);
        };
        enqueue_all(std::make_index_sequence<N>{});
    }

    unlock_all();

    // 2.4 Register cancellable timer(s) for TimeoutCase(s)
    {
        auto register_timers = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            (([&] {
                auto& c = std::get<Is>(tuple_cases);
                using CType = std::decay_t<decltype(c)>;
                if constexpr (detail::IsTimeoutCase<CType>::value) {
                    has_any_waiter = true;
                    auto deadline = std::chrono::steady_clock::now() +
                                    std::chrono::duration_cast<std::chrono::steady_clock::duration>(c.duration);
                    int case_idx = static_cast<int>(Is);
                    std::uint64_t tid = timer_rt->schedule_timer_callback(
                        deadline,
                        [&winner, &park_lock, waiter, case_idx]() mutable {
                            int expected = -1;
                            if (winner.compare_exchange_strong(expected, case_idx, std::memory_order_acq_rel)) {
                                park_lock.lock();
                                park_lock.unlock();
                                waiter.unpark();
                            }
                        }
                    );
                    timer_ids[timer_count++] = tid;
                }
            }()), ...);
        };
        register_timers(std::make_index_sequence<N>{});
    }

    if (!has_any_waiter) {
        // All channels were already closed and no timeout/default clause exists — avoid deadlocking forever
        park_lock.unlock();
        return;
    }

    // 2.5 Atomically park until one channel or timer wins `winner`!
    waiter.park_current(park_lock);

    // ---- WOKEN UP ----
    // 2.6 Immediately cancel any pending timeout timers and unlink all losing waiters from every channel!
    for (std::size_t i = 0; i < timer_count; ++i) {
        timer_rt->cancel_timer(timer_ids[i]);
    }
    {
        auto unlink_all = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            (([&] {
                auto& c = std::get<Is>(tuple_cases);
                using CType = std::decay_t<decltype(c)>;
                if constexpr (!detail::IsDefaultCase<CType>::value &&
                              !detail::IsTimeoutCase<CType>::value) {
                    if (!c.closed_skipped) {
                        c.unlink();
                    }
                }
            }()), ...);
        };
        unlink_all(std::make_index_sequence<N>{});
    }

    // 2.7 Invoke the winning case's callback!
    int won = winner.load(std::memory_order_acquire);
    {
        auto dispatch_winner = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            (([&] {
                if (won == static_cast<int>(Is)) {
                    auto& c = std::get<Is>(tuple_cases);
                    using CType = std::decay_t<decltype(c)>;
                    if constexpr (detail::IsTimeoutCase<CType>::value) {
                        c.callback();
                    } else if constexpr (!detail::IsDefaultCase<CType>::value) {
                        c.invoke_winner();
                    }
                }
            }()), ...);
        };
        dispatch_winner(std::make_index_sequence<N>{});
    }
}

} // namespace gorun
