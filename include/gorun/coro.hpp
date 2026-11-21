#pragma once

/**
 * @file coro.hpp
 * @brief Bidirectional C++20 Coroutines (`co_await`) bridge for `gorun`:
 *        1. `co_await JoinHandle<T>` & `co_await JoinHandle<void>` (suspends coroutine cleanly without blocking OS threads).
 *        2. `co_await gorun::co_recv(ch)` & `co_await gorun::co_send(ch, val)` for awaiting `gorun::Channel<T>`.
 *        3. `gorun::Task<T>` lightweight C++20 coroutine task type.
 *        4. `gorun::await_coro(awaitable)` — parks a `gorun` green routine while awaiting ANY external C++20 `co_await`-able!
 */

#include <coroutine>
#include <exception>
#include <memory>
#include <optional>
#include <semaphore>
#include <type_traits>
#include <utility>

#include "gorun/channel.hpp"
#include "gorun/runtime.hpp"
#include "gorun/task_group.hpp"

namespace gorun {

// ============================================================================
// 1. C++20 Awaiter for `JoinHandle<T>` and `JoinHandle<void>`
// ============================================================================

template <typename T>
struct JoinHandleAwaiter {
    std::shared_ptr<typename JoinHandle<T>::SharedState> state;

    [[nodiscard]] bool await_ready() const noexcept {
        return !state || state->ready.load(std::memory_order_acquire);
    }

    bool await_suspend(std::coroutine_handle<> handle) const {
        if (!state) return false;
        // Resumes the suspended C++20 coroutine on the gorun runtime once the green routine finishes!
        return state->add_continuation([handle]() mutable {
            default_runtime().spawn_raw([handle]() mutable {
                handle.resume();
            });
        });
    }

    T await_resume() {
        if (state->exception) {
            std::rethrow_exception(state->exception);
        }
        return std::move(*state->value);
    }
};

template <>
struct JoinHandleAwaiter<void> {
    std::shared_ptr<JoinHandle<void>::SharedState> state;

    [[nodiscard]] bool await_ready() const noexcept {
        return !state || state->ready.load(std::memory_order_acquire);
    }

    bool await_suspend(std::coroutine_handle<> handle) const {
        if (!state) return false;
        return state->add_continuation([handle]() mutable {
            default_runtime().spawn_raw([handle]() mutable {
                handle.resume();
            });
        });
    }

    void await_resume() const {
        if (state && state->exception) {
            std::rethrow_exception(state->exception);
        }
    }
};

/// Enables `auto val = co_await gorun::go_async(...);` and `co_await gorun::spawn_blocking(...);`!
template <typename T>
inline JoinHandleAwaiter<T> operator co_await(const JoinHandle<T>& handle) noexcept {
    return JoinHandleAwaiter<T>{handle.shared_state()};
}

template <typename T>
inline JoinHandleAwaiter<T> operator co_await(JoinHandle<T>&& handle) noexcept {
    return JoinHandleAwaiter<T>{handle.shared_state()};
}

// ============================================================================
// 2. C++20 Awaitables for `gorun::Channel<T>` (`co_recv` / `co_send`)
// ============================================================================

template <typename T>
struct ChannelRecvAwaiter {
    Channel<T>* ch;
    std::optional<T> result{};

    [[nodiscard]] bool await_ready() {
        bool closed_empty = false;
        if (ch->try_recv(result, closed_empty)) {
            return true;
        }
        if (closed_empty) {
            result.reset();
            return true;
        }
        return false;
    }

    void await_suspend(std::coroutine_handle<> handle) {
        // Offload the parking channel recv onto a lightweight green routine (zero OS-thread blocking!)
        default_runtime().spawn_raw([this, handle]() mutable {
            result = ch->recv();
            handle.resume();
        }, static_cast<std::size_t>(StackSize::Small));
    }

    std::optional<T> await_resume() noexcept(std::is_nothrow_move_constructible_v<T>) {
        return std::move(result);
    }
};

template <typename T>
struct ChannelSendAwaiter {
    Channel<T>* ch;
    T value;
    bool ok{false};

    [[nodiscard]] bool await_ready() {
        bool closed = false;
        if (ch->try_send(value, closed)) {
            ok = true;
            return true;
        }
        if (closed) {
            ok = false;
            return true;
        }
        return false;
    }

    void await_suspend(std::coroutine_handle<> handle) {
        default_runtime().spawn_raw([this, handle]() mutable {
            ok = ch->send(std::move(value));
            handle.resume();
        }, static_cast<std::size_t>(StackSize::Small));
    }

    bool await_resume() const noexcept {
        return ok;
    }
};

/// C++20 awaitable for non-blocking receive from a `gorun::Channel<T>`: `auto v = co_await gorun::co_recv(ch);`
template <typename T>
[[nodiscard]] inline ChannelRecvAwaiter<T> co_recv(Channel<T>& ch) noexcept {
    return ChannelRecvAwaiter<T>{&ch, std::nullopt};
}

/// C++20 awaitable for non-blocking send to a `gorun::Channel<T>`: `bool ok = co_await gorun::co_send(ch, val);`
template <typename T>
[[nodiscard]] inline ChannelSendAwaiter<T> co_send(Channel<T>& ch, T value) {
    return ChannelSendAwaiter<T>{&ch, std::move(value), false};
}

// ============================================================================
// 3. Lightweight Standard C++20 `gorun::Task<T>` Coroutine Type
// ============================================================================

template <typename T = void>
class Task {
public:
    struct promise_type {
        std::optional<T> value{};
        std::exception_ptr exception{nullptr};
        std::coroutine_handle<> continuation{};

        Task get_return_object() noexcept {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_always initial_suspend() noexcept { return {}; }

        struct FinalAwaiter {
            bool await_ready() const noexcept { return false; }
            std::coroutine_handle<> await_suspend(std::coroutine_handle<promise_type> h) noexcept {
                if (h.promise().continuation) {
                    return h.promise().continuation;
                }
                return std::noop_coroutine();
            }
            void await_resume() const noexcept {}
        };
        FinalAwaiter final_suspend() noexcept { return {}; }

        void return_value(T v) { value.emplace(std::move(v)); }
        void unhandled_exception() noexcept { exception = std::current_exception(); }
    };

private:
    std::coroutine_handle<promise_type> handle_{};

public:
    explicit Task(std::coroutine_handle<promise_type> h) noexcept : handle_(h) {}
    Task(Task&& other) noexcept : handle_(std::exchange(other.handle_, {})) {}
    Task& operator=(Task&& other) noexcept {
        if (this != &other) {
            if (handle_) handle_.destroy();
            handle_ = std::exchange(other.handle_, {});
        }
        return *this;
    }
    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;

    ~Task() {
        if (handle_) handle_.destroy();
    }

    struct Awaiter {
        std::coroutine_handle<promise_type> handle;

        bool await_ready() const noexcept { return !handle || handle.done(); }
        std::coroutine_handle<> await_suspend(std::coroutine_handle<> caller) noexcept {
            handle.promise().continuation = caller;
            return handle; // Symmetric transfer directly into child coroutine!
        }
        T await_resume() {
            if (handle.promise().exception) {
                std::rethrow_exception(handle.promise().exception);
            }
            return std::move(*handle.promise().value);
        }
    };

    Awaiter operator co_await() && noexcept { return Awaiter{handle_}; }
    Awaiter operator co_await() & noexcept { return Awaiter{handle_}; }
};

template <>
class Task<void> {
public:
    struct promise_type {
        std::exception_ptr exception{nullptr};
        std::coroutine_handle<> continuation{};

        Task get_return_object() noexcept {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_always initial_suspend() noexcept { return {}; }

        struct FinalAwaiter {
            bool await_ready() const noexcept { return false; }
            std::coroutine_handle<> await_suspend(std::coroutine_handle<promise_type> h) noexcept {
                if (h.promise().continuation) {
                    return h.promise().continuation;
                }
                return std::noop_coroutine();
            }
            void await_resume() const noexcept {}
        };
        FinalAwaiter final_suspend() noexcept { return {}; }

        void return_void() noexcept {}
        void unhandled_exception() noexcept { exception = std::current_exception(); }
    };

private:
    std::coroutine_handle<promise_type> handle_{};

public:
    explicit Task(std::coroutine_handle<promise_type> h) noexcept : handle_(h) {}
    Task(Task&& other) noexcept : handle_(std::exchange(other.handle_, {})) {}
    Task& operator=(Task&& other) noexcept {
        if (this != &other) {
            if (handle_) handle_.destroy();
            handle_ = std::exchange(other.handle_, {});
        }
        return *this;
    }
    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;

    ~Task() {
        if (handle_) handle_.destroy();
    }

    struct Awaiter {
        std::coroutine_handle<promise_type> handle;

        bool await_ready() const noexcept { return !handle || handle.done(); }
        std::coroutine_handle<> await_suspend(std::coroutine_handle<> caller) noexcept {
            handle.promise().continuation = caller;
            return handle;
        }
        void await_resume() const {
            if (handle.promise().exception) {
                std::rethrow_exception(handle.promise().exception);
            }
        }
    };

    Awaiter operator co_await() && noexcept { return Awaiter{handle_}; }
    Awaiter operator co_await() & noexcept { return Awaiter{handle_}; }
};

// ============================================================================
// 4. Bridge: Await ANY C++20 `co_await`-able Inside a `gorun` Green Routine!
// ============================================================================
namespace detail {

struct CoroSyncSignal {
    SpinLock lock{};
    bool done{false};
    UniversalWaiter* waiter{nullptr};

    void signal() noexcept {
        lock.lock();
        done = true;
        UniversalWaiter* w = waiter;
        lock.unlock();
        if (w) {
            w->unpark();
        }
    }

    void wait() noexcept {
        lock.lock();
        if (!done) {
            std::binary_semaphore sem{0};
            auto w = UniversalWaiter::make_for_current(sem);
            waiter = &w;
            w.park_current(lock);
        } else {
            lock.unlock();
        }
    }
};

struct DetachedCoroDriver {
    struct promise_type {
        DetachedCoroDriver get_return_object() noexcept { return {}; }
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() noexcept {}
        void unhandled_exception() noexcept {}
    };
};

template <typename Awaitable>
auto get_awaiter(Awaitable&& aw) {
    if constexpr (requires { std::forward<Awaitable>(aw).operator co_await(); }) {
        return std::forward<Awaitable>(aw).operator co_await();
    } else if constexpr (requires { operator co_await(std::forward<Awaitable>(aw)); }) {
        return operator co_await(std::forward<Awaitable>(aw));
    } else {
        return std::forward<Awaitable>(aw);
    }
}

template <typename Awaitable>
using AwaitResultT = decltype(get_awaiter(std::declval<Awaitable>()).await_resume());

template <typename Awaitable, typename R>
DetachedCoroDriver drive_awaitable_value(Awaitable aw,
                                         std::optional<R>& out_val,
                                         std::exception_ptr& out_exc,
                                         CoroSyncSignal& sig) {
    try {
        out_val.emplace(co_await std::move(aw));
    } catch (...) {
        out_exc = std::current_exception();
    }
    sig.signal();
}

template <typename Awaitable>
DetachedCoroDriver drive_awaitable_void(Awaitable aw,
                                        std::exception_ptr& out_exc,
                                        CoroSyncSignal& sig) {
    try {
        co_await std::move(aw);
    } catch (...) {
        out_exc = std::current_exception();
    }
    sig.signal();
}

} // namespace detail

/**
 * @brief Awaits ANY C++20 `co_await`-able (e.g., `gorun::Task<T>` or external coroutine library task)
 *        synchronously inside a `gorun` green routine — parking the green routine without blocking OS workers!
 */
template <typename Awaitable>
auto await_coro(Awaitable&& aw) -> detail::AwaitResultT<Awaitable> {
    using R = detail::AwaitResultT<Awaitable>;
    detail::CoroSyncSignal sig;
    std::exception_ptr exc{nullptr};

    if constexpr (std::is_void_v<R>) {
        detail::drive_awaitable_void(std::forward<Awaitable>(aw), exc, sig);
        sig.wait();
        if (exc) {
            std::rethrow_exception(exc);
        }
    } else {
        std::optional<R> result;
        detail::drive_awaitable_value(std::forward<Awaitable>(aw), result, exc, sig);
        sig.wait();
        if (exc) {
            std::rethrow_exception(exc);
        }
        return std::move(*result);
    }
}

} // namespace gorun
