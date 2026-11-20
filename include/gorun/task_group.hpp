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

    T get() {
        state_->wg.wait();
        if (state_->exception) {
            std::rethrow_exception(state_->exception);
        }
        return std::move(*state_->value);
    }

    void join() {
        (void)get();
    }
};

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

    void get() {
        state_->wg.wait();
        if (state_->exception) {
            std::rethrow_exception(state_->exception);
        }
    }

    void join() {
        get();
    }
};

/**
 * @class TaskGroup
 * @brief Structured concurrency scope (inspired by Go's `errgroup.Group` and C++26 `std::structured_task_group`).
 *
 * - Stores `TaskGroup*` directly in `implicit_scope` (no invalid derived downcast UB!).
 * - Supports move-only callables (lambdas capturing `std::unique_ptr`).
 * - Automatically cancels sibling routines via `std::stop_token` if the enclosing scope unwinds due to an exception.
 */
class TaskGroup {
private:
    Runtime& runtime_;
    WaitGroup wg_;
    detail::SpinLock exc_lock_;
    std::exception_ptr first_exception_{nullptr};
    std::stop_source stop_source_;
    TaskGroup* prev_scope_{nullptr};
    int initial_uncaught_exceptions_{0};

public:
    explicit TaskGroup(Runtime& rt = default_runtime()) noexcept
        : runtime_(rt)
        , initial_uncaught_exceptions_(std::uncaught_exceptions()) {
        prev_scope_ = detail::set_implicit_scope(this);
    }

    ~TaskGroup() noexcept {
        detail::set_implicit_scope(prev_scope_);
        // If parent block is unwinding due to a newly thrown exception, immediately signal children to stop!
        if (std::uncaught_exceptions() > initial_uncaught_exceptions_) {
            stop_source_.request_stop();
        }
        wg_.wait();
    }

    TaskGroup(const TaskGroup&) = delete;
    TaskGroup& operator=(const TaskGroup&) = delete;

    template <typename F>
    void spawn(F&& fn) {
        wg_.add(1);
        try {
            runtime_.spawn_raw([this, f = std::forward<F>(fn)]() mutable {
                TaskGroup* prev = detail::set_implicit_scope(this);
                try {
                    if constexpr (std::is_invocable_v<std::decay_t<F>&, std::stop_token>) {
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
        } catch (...) {
            wg_.done();
            throw;
        }
    }

    void cancel() noexcept {
        stop_source_.request_stop();
    }

    [[nodiscard]] std::stop_token get_stop_token() const noexcept {
        return stop_source_.get_token();
    }

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

    void join() {
        wait();
    }
};

/**
 * @class Scope
 * @brief Ergonomic RAII scope for structured concurrency with `gorun::go([&]{ ... })`.
 */
class Scope : public TaskGroup {
public:
    using TaskGroup::TaskGroup;
};

template <typename F>
void go(F&& fn) {
    if (TaskGroup* active_scope = detail::current_implicit_scope()) {
        active_scope->spawn(std::forward<F>(fn));
    } else {
        default_runtime().spawn_raw(std::forward<F>(fn));
    }
}

template <typename F>
void go_in(TaskGroup& group, F&& fn) {
    group.spawn(std::forward<F>(fn));
}

template <typename F>
void go_in(Runtime& rt, F&& fn) {
    rt.spawn_raw(std::forward<F>(fn));
}

template <std::invocable F>
auto go_async(F&& fn) -> JoinHandle<std::invoke_result_t<F>> {
    using R = std::invoke_result_t<F>;
    using State = typename JoinHandle<R>::SharedState;
    auto state = std::make_shared<State>();
    state->wg.add(1);

    try {
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
    } catch (...) {
        state->wg.done();
        throw;
    }

    return JoinHandle<R>(std::move(state));
}

} // namespace gorun
