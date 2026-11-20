#pragma once

/**
 * @file task_group.hpp
 * @brief Structured concurrency (`TaskGroup`, `Scope`, `JoinHandle<T>`) and ergonomic `go(...)` launchers.
 */

#include <exception>
#include <functional>
#include <memory>
#include <optional>
#include <stop_token>
#include <type_traits>
#include <utility>

#include "gorun/runtime.hpp"
#include "gorun/sync.hpp"

namespace gorun {

/**
 * @class JoinHandle
 * @brief Handle to an asynchronously spawned green routine (non-OS-blocking equivalent of `std::future<T>`).
 *
 * Allows waiting for completion via `get()` or `join()`, retrieving the computed value `T` or rethrowing
 * any unhandled exception thrown inside the green routine.
 *
 * @tparam T Return type of the green routine.
 */
template <typename T>
class JoinHandle {
public:
    struct SharedState {
        WaitGroup wg{};
        std::optional<T> value{};
        std::exception_ptr exception{nullptr};
    };

private:
    std::shared_ptr<SharedState> state_;

public:
    explicit JoinHandle(std::shared_ptr<SharedState> s) noexcept
        : state_(std::move(s)) {}

    /**
     * @brief Parks the calling routine (or OS thread) until the task completes and returns its result.
     * @return Computed value of type `T`.
     * @throws Any exception thrown and uncaught inside the spawned routine.
     */
    T get() {
        state_->wg.wait();
        if (state_->exception) {
            std::rethrow_exception(state_->exception);
        }
        return std::move(*state_->value);
    }

    /**
     * @brief Waits for the routine to finish (rethrowing any exception if one occurred).
     */
    void join() {
        (void)get();
    }
};

/**
 * @brief Specialization of `JoinHandle<void>` for routines returning `void`.
 */
template <>
class JoinHandle<void> {
public:
    struct SharedState {
        WaitGroup wg{};
        std::exception_ptr exception{nullptr};
    };

private:
    std::shared_ptr<SharedState> state_;

public:
    explicit JoinHandle(std::shared_ptr<SharedState> s) noexcept
        : state_(std::move(s)) {}

    /**
     * @brief Parks the calling routine until completion and rethrows any stored exception.
     */
    void get() {
        state_->wg.wait();
        if (state_->exception) {
            std::rethrow_exception(state_->exception);
        }
    }

    /**
     * @brief Alias for `get()`.
     */
    void join() {
        get();
    }
};

/**
 * @class TaskGroup
 * @brief Structured concurrency scope (inspired by Go's `golang.org/x/sync/errgroup.Group` and C++26 `std::structured_task_group`).
 *
 * Key safety guarantees:
 * 1. **Lifetime Safety (No Dangling `[&]` Captures):** Unlike Go (where the compiler's escape analysis moves
 *    captured local variables to the garbage-collected heap), C++ stack frames are destroyed upon scope exit.
 *    `~TaskGroup()` guarantees that all child routines finish before the scope exits, making `[&]` reference captures 100% safe.
 * 2. **Implicit `go(...)` Attachment:** While a `TaskGroup` (or `Scope`) is active on the current routine/thread,
 *    even plain `gorun::go([&]{ ... })` calls (including transitively nested ones!) automatically attach to this group.
 * 3. **Exception Propagation & Cooperative Cancellation:** If any child routine throws an exception, `TaskGroup`
 *    captures the first exception into a `std::exception_ptr`, automatically signals `cancel()` via `std::stop_token`
 *    to all sibling routines, and rethrows the exception upon `wait()`.
 */
class TaskGroup {
private:
    Runtime& runtime_;
    WaitGroup wg_;
    detail::SpinLock exc_lock_;
    std::exception_ptr first_exception_{nullptr};
    std::stop_source stop_source_;
    Scope* prev_scope_{nullptr};

public:
    /**
     * @brief Opens a structured task group bound to `rt` (defaults to `gorun::default_runtime()`).
     */
    explicit TaskGroup(Runtime& rt = default_runtime()) noexcept
        : runtime_(rt) {
        prev_scope_ = detail::set_implicit_scope(reinterpret_cast<Scope*>(this));
    }

    /**
     * @brief Automatically waits for all child routines to complete before leaving the lexical block.
     */
    ~TaskGroup() noexcept {
        detail::set_implicit_scope(prev_scope_);
        wg_.wait();
    }

    TaskGroup(const TaskGroup&) = delete;
    TaskGroup& operator=(const TaskGroup&) = delete;

    /**
     * @brief Spawns a child green routine inside this group.
     *
     * The callable `fn` may take zero arguments (`[]() { ... }`) or accept a cooperative cancellation
     * token (`[](std::stop_token st) { ... }`).
     *
     * @tparam F Callable type.
     * @param fn Routine body.
     */
    template <typename F>
    void spawn(F&& fn) {
        wg_.add(1);
        runtime_.spawn_raw([this, f = std::forward<F>(fn)]() mutable {
            auto* prev = detail::set_implicit_scope(reinterpret_cast<Scope*>(this));
            try {
                if constexpr (std::is_invocable_v<F, std::stop_token>) {
                    std::invoke(f, stop_source_.get_token());
                } else {
                    std::invoke(f);
                }
            } catch (...) {
                stop_source_.request_stop();
                std::lock_guard g(exc_lock_);
                if (!first_exception_) {
                    first_exception_ = std::current_exception();
                }
            }
            detail::set_implicit_scope(prev);
            wg_.done();
        });
    }

    /**
     * @brief Signals cooperative cancellation (`request_stop()`) to all routines in this group.
     */
    void cancel() noexcept {
        stop_source_.request_stop();
    }

    /**
     * @brief Returns the `std::stop_token` associated with this `TaskGroup`.
     */
    [[nodiscard]] std::stop_token get_stop_token() const noexcept {
        return stop_source_.get_token();
    }

    /**
     * @brief Parks the current routine until all child routines complete, rethrowing the first captured exception if any occurred.
     */
    void wait() {
        wg_.wait();
        std::exception_ptr exc_to_throw{nullptr};
        {
            std::lock_guard g(exc_lock_);
            exc_to_throw = std::exchange(first_exception_, nullptr);
        }
        if (exc_to_throw) {
            std::rethrow_exception(exc_to_throw);
        }
    }

    /**
     * @brief Alias for `wait()`.
     */
    void join() {
        wait();
    }
};

/**
 * @class Scope
 * @brief Ergonomic RAII scope for structured concurrency with `gorun::go([&]{ ... })`.
 *
 * Any `gorun::go(...)` call inside a block where `gorun::Scope scope;` is declared will automatically
 * register with `scope` and is guaranteed to complete before `scope` is destroyed.
 */
class Scope : public TaskGroup {
public:
    using TaskGroup::TaskGroup;
};

/**
 * @brief Spawns a green routine with the ergonomics of Go's `go` keyword.
 *
 * - If called inside an active `gorun::Scope` or `gorun::TaskGroup`, automatically attaches to that scope
 *   (making `[&]` reference captures safe).
 * - Otherwise, spawns onto `gorun::default_runtime()` (lazily initializing the global worker pool on first call).
 *
 * @tparam F Callable type (`void()` or `void(std::stop_token)`).
 * @param fn Routine body.
 */
template <typename F>
void go(F&& fn) {
    if (Scope* active_scope = detail::current_implicit_scope()) {
        active_scope->spawn(std::forward<F>(fn));
    } else {
        default_runtime().spawn_raw(std::forward<F>(fn));
    }
}

/**
 * @brief Explicitly spawns a green routine inside the specified `TaskGroup` / `Scope`.
 */
template <typename F>
void go_in(TaskGroup& group, F&& fn) {
    group.spawn(std::forward<F>(fn));
}

/**
 * @brief Explicitly spawns a green routine inside the specified `Runtime` instance.
 */
template <typename F>
void go_in(Runtime& rt, F&& fn) {
    rt.spawn_raw(std::forward<F>(fn));
}

/**
 * @brief Spawns a green routine and returns a `JoinHandle<R>` to await its return value or exception.
 *
 * @tparam F Nullary callable type.
 * @param fn Task body.
 * @return `JoinHandle<std::invoke_result_t<F>>`.
 */
template <std::invocable F>
auto go_async(F&& fn) -> JoinHandle<std::invoke_result_t<F>> {
    using R = std::invoke_result_t<F>;
    using State = typename JoinHandle<R>::SharedState;
    auto state = std::make_shared<State>();
    state->wg.add(1);

    default_runtime().spawn_raw([state, f = std::forward<F>(fn)]() mutable {
        try {
            if constexpr (std::is_void_v<R>) {
                std::invoke(f);
            } else {
                state->value.emplace(std::invoke(f));
            }
        } catch (...) {
            state->exception = std::current_exception();
        }
        state->wg.done();
    });

    return JoinHandle<R>(std::move(state));
}

} // namespace gorun
