#pragma once

/**
 * @file channel.hpp
 * @brief Typed message-passing channels `gorun::Channel<T>` (buffered and unbuffered rendezvous)
 *        and the `gorun::select(...)` multiplexer.
 */

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <optional>
#include <random>
#include <semaphore>
#include <tuple>
#include <utility>

#include "gorun/runtime.hpp"

namespace gorun {

namespace detail {

/**
 * @struct ChannelWaiter
 * @brief Intrusive stack-allocated waiter node for a blocked sender or receiver on a `Channel<T>` (analogous to Go's `sudog`).
 *
 * Holds a direct pointer (`data_slot`) to the `T` or `std::optional<T>` variable on the parked routine's stack frame,
 * enabling zero-allocation **Direct Stack-to-Stack Handoff** via `std::move`.
 */
template <typename T>
struct ChannelWaiter {
    UniversalWaiter waiter{};
    void* data_slot{nullptr};
    bool woken_by_close{false};

    std::atomic<int>* select_winner{nullptr};
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
};

} // namespace detail

/**
 * @class Channel
 * @brief Typed MPMC (Multi-Producer Multi-Consumer) channel modeled directly on Go's `chan T` (`runtime.hchan`).
 *
 * Supports two operating modes:
 * - **Unbuffered Rendezvous (`capacity == 0`):** Senders and receivers meet and move values directly between
 *   their respective routine stacks without touching any intermediate ring buffer.
 * - **Buffered (`capacity > 0`):** Backed by a pre-allocated circular ring buffer. Senders park only when the
 *   buffer is full; receivers park only when the buffer is empty.
 *
 * @tparam T Payload type transmitted across the channel (supports move-only types such as `std::unique_ptr<U>`).
 */
template <typename T>
class Channel {
private:
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
            if (r->try_claim_for_select()) {
                return r;
            }
        }
        return nullptr;
    }

    detail::ChannelWaiter<T>* pop_valid_sender() noexcept {
        while (auto* s = senders_.pop_front()) {
            if (s->try_claim_for_select()) {
                return s;
            }
        }
        return nullptr;
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
        lock_.lock();
        if (closed_) {
            is_closed_out = true;
            lock_.unlock();
            return false;
        }
        is_closed_out = false;

        if (auto* receiver = pop_valid_receiver()) {
            auto* dst = static_cast<std::optional<T>*>(receiver->data_slot);
            dst->emplace(std::move(value));
            auto to_wake = receiver->waiter;
            lock_.unlock();
            to_wake.unpark();
            return true;
        }

        if (count_ < capacity_) {
            buffer_[tail_].emplace(std::move(value));
            tail_ = (tail_ + 1) % capacity_;
            ++count_;
            lock_.unlock();
            return true;
        }

        lock_.unlock();
        return false;
    }

    /**
     * @brief Non-blocking attempt to receive a value from the channel.
     * @param out_val Destination `std::optional<T>` receiving the value.
     * @param is_closed_empty_out Set to `true` if the channel is closed and drained.
     * @return `true` if a value was received or the channel is closed and empty.
     */
    bool try_recv(std::optional<T>& out_val, bool& is_closed_empty_out) {
        lock_.lock();
        is_closed_empty_out = false;

        if (count_ > 0) {
            out_val = std::move(buffer_[head_]);
            buffer_[head_].reset();
            head_ = (head_ + 1) % capacity_;
            --count_;

            if (auto* sender = pop_valid_sender()) {
                T* src = static_cast<T*>(sender->data_slot);
                buffer_[tail_].emplace(std::move(*src));
                tail_ = (tail_ + 1) % capacity_;
                ++count_;
                auto to_wake = sender->waiter;
                lock_.unlock();
                to_wake.unpark();
                return true;
            }
            lock_.unlock();
            return true;
        }

        if (auto* sender = pop_valid_sender()) {
            T* src = static_cast<T*>(sender->data_slot);
            out_val.emplace(std::move(*src));
            auto to_wake = sender->waiter;
            lock_.unlock();
            to_wake.unpark();
            return true;
        }

        if (closed_) {
            is_closed_empty_out = true;
            out_val = std::nullopt;
            lock_.unlock();
            return true;
        }

        lock_.unlock();
        return false;
    }

    /**
     * @brief Sends `value` into the channel (`ch <- value` in Go).
     *
     * Parks the calling green routine if the buffer is full (or if unbuffered and no receiver is waiting).
     *
     * @param value Value to transmit (moved into the receiver or buffer).
     * @return `true` if delivered; `false` if the channel is closed.
     */
    bool send(T value) {
        lock_.lock();

        if (closed_) {
            lock_.unlock();
            return false;
        }

        if (auto* receiver = pop_valid_receiver()) {
            auto* dst = static_cast<std::optional<T>*>(receiver->data_slot);
            dst->emplace(std::move(value));
            auto to_wake = receiver->waiter;
            lock_.unlock();
            to_wake.unpark();
            return true;
        }

        if (count_ < capacity_) {
            buffer_[tail_].emplace(std::move(value));
            tail_ = (tail_ + 1) % capacity_;
            ++count_;
            lock_.unlock();
            return true;
        }

        std::binary_semaphore fallback_sem{0};
        detail::ChannelWaiter<T> node{
            .waiter = detail::UniversalWaiter::make_for_current(fallback_sem),
            .data_slot = &value,
            .woken_by_close = false
        };

        senders_.push_back(&node);
        node.waiter.park_current(lock_);

        return !node.woken_by_close;
    }

    /**
     * @brief Receives the next value from the channel (`v, ok := <-ch` in Go).
     *
     * Parks the calling green routine if the channel is currently empty.
     *
     * @return `std::optional<T>` containing the received item, or `std::nullopt` once the channel is closed and drained.
     */
    std::optional<T> recv() {
        lock_.lock();

        if (count_ > 0) {
            std::optional<T> result = std::move(buffer_[head_]);
            buffer_[head_].reset();
            head_ = (head_ + 1) % capacity_;
            --count_;

            if (auto* sender = pop_valid_sender()) {
                T* src = static_cast<T*>(sender->data_slot);
                buffer_[tail_].emplace(std::move(*src));
                tail_ = (tail_ + 1) % capacity_;
                ++count_;
                auto to_wake = sender->waiter;
                lock_.unlock();
                to_wake.unpark();
                return result;
            }

            lock_.unlock();
            return result;
        }

        if (auto* sender = pop_valid_sender()) {
            T* src = static_cast<T*>(sender->data_slot);
            std::optional<T> result(std::move(*src));
            auto to_wake = sender->waiter;
            lock_.unlock();
            to_wake.unpark();
            return result;
        }

        if (closed_) {
            lock_.unlock();
            return std::nullopt;
        }

        std::optional<T> result_slot;
        std::binary_semaphore fallback_sem{0};
        detail::ChannelWaiter<T> node{
            .waiter = detail::UniversalWaiter::make_for_current(fallback_sem),
            .data_slot = &result_slot,
            .woken_by_close = false
        };

        receivers_.push_back(&node);
        node.waiter.park_current(lock_);

        if (node.woken_by_close) {
            return std::nullopt;
        }
        return result_slot;
    }

    /**
     * @brief Closes the channel (`close(ch)` in Go) and wakes all parked readers and writers.
     *
     * Parked readers receive `std::nullopt` once all buffered elements have been consumed; parked writers return `false`.
     */
    void close() noexcept {
        std::vector<detail::UniversalWaiter> to_wake;

        lock_.lock();
        if (closed_) {
            lock_.unlock();
            return;
        }
        closed_ = true;

        while (auto* r = receivers_.pop_front()) {
            if (r->try_claim_for_select()) {
                r->woken_by_close = true;
                to_wake.push_back(r->waiter);
            }
        }
        while (auto* s = senders_.pop_front()) {
            if (s->try_claim_for_select()) {
                s->woken_by_close = true;
                to_wake.push_back(s->waiter);
            }
        }
        lock_.unlock();

        for (auto& w : to_wake) {
            w.unpark();
        }
    }

    /**
     * @brief Checks whether `close()` has been called on this channel.
     */
    [[nodiscard]] bool is_closed() noexcept {
        std::lock_guard g(lock_);
        return closed_;
    }

    /**
     * @brief Returns the current number of buffered elements (`len(ch)` in Go).
     */
    [[nodiscard]] std::size_t size() noexcept {
        std::lock_guard g(lock_);
        return count_;
    }

    /**
     * @brief Returns the buffer capacity (`cap(ch)` in Go).
     */
    [[nodiscard]] std::size_t capacity() const noexcept {
        return capacity_;
    }

    /**
     * @brief Creates a receive clause for `gorun::select(...)`.
     * @param cb Callback invoked with `T` or `std::optional<T>` when this clause is selected.
     */
    template <typename Callback>
    auto on_recv(Callback&& cb);

    /**
     * @brief Creates a send clause for `gorun::select(...)`.
     * @param val Value to send if this clause is selected.
     * @param cb Callback invoked after the value is sent.
     */
    template <typename Callback>
    auto on_send(T val, Callback&& cb);
};

template <typename T, typename Callback>
struct RecvCase {
    Channel<T>* channel;
    Callback callback;

    bool try_execute() {
        std::optional<T> val;
        bool closed_empty = false;
        if (channel->try_recv(val, closed_empty)) {
            if constexpr (std::is_invocable_v<Callback, std::optional<T>>) {
                callback(std::move(val));
            } else if (!closed_empty && val.has_value()) {
                callback(std::move(*val));
            }
            return true;
        }
        return false;
    }
};

template <typename T, typename Callback>
struct SendCase {
    Channel<T>* channel;
    T value;
    Callback callback;

    bool try_execute() {
        bool is_closed = false;
        if (channel->try_send(value, is_closed)) {
            if constexpr (std::is_invocable_v<Callback, bool>) {
                callback(true);
            } else {
                callback();
            }
            return true;
        }
        return false;
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

/**
 * @brief Creates a non-blocking `default` clause for `gorun::select(...)`.
 */
template <typename Callback>
auto on_default(Callback&& cb) {
    return DefaultCase<std::decay_t<Callback>>{std::forward<Callback>(cb)};
}

/**
 * @brief Creates a timeout clause for `gorun::select(...)` (analogous to `case <-time.After(d):` in Go).
 */
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

} // namespace detail

/**
 * @brief Multi-channel `select` statement modeled on Go's `select { ... }` (`runtime.selectgo`).
 *
 * Polls all channel clauses in pseudo-random order to guarantee fairness and prevent starvation,
 * executing the callback of the first ready clause (or `on_default` / `on_timeout`).
 *
 * @code
 * gorun::select(
 *     ch1.on_recv([](int v) { ... }),
 *     ch2.on_send(42, []() { ... }),
 *     gorun::on_timeout(50ms, []() { ... })
 * );
 * @endcode
 */
template <typename... Cases>
void select(Cases&&... cases) {
    auto tuple_cases = std::make_tuple(std::forward<Cases>(cases)...);
    constexpr std::size_t N = sizeof...(Cases);

    std::array<std::size_t, N> order{};
    for (std::size_t i = 0; i < N; ++i) order[i] = i;

    thread_local std::mt19937 rng{std::random_device{}()};
    std::shuffle(order.begin(), order.end(), rng);

    auto start_time = std::chrono::steady_clock::now();

    for (;;) {
        bool executed = false;

        for (std::size_t idx : order) {
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

        bool has_finish = false;
        auto check_special = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            (([&] {
                if (has_finish) return;
                auto& c = std::get<Is>(tuple_cases);
                using CType = std::decay_t<decltype(c)>;
                if constexpr (detail::IsDefaultCase<CType>::value) {
                    c.callback();
                    has_finish = true;
                } else if constexpr (detail::IsTimeoutCase<CType>::value) {
                    if (std::chrono::steady_clock::now() - start_time >= c.duration) {
                        c.callback();
                        has_finish = true;
                    }
                }
            }()), ...);
        };
        check_special(std::make_index_sequence<N>{});
        if (has_finish) return;

        this_routine::yield();
    }
}

} // namespace gorun
