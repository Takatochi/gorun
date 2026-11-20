#pragma once

/**
 * @file defer.hpp
 * @brief Zero-overhead RAII scope-exit action (`gorun::defer` and `GORUN_DEFER`), modeled on Go's `defer` statement.
 *
 * Unlike Go's runtime-managed `runtime._defer` linked list (which operates at function boundary),
 * `gorun::defer` leverages C++ deterministic block-scoped RAII destructors:
 * - **Zero heap allocations** (entirely inlined on the routine's stack frame).
 * - **LIFO execution order** when multiple `defer` guards are declared in the same scope (matching Go's LIFO `defer` stack).
 * - **Exception-safe cleanup**: executes deterministically whether the scope exits normally or via stack unwinding (`throw`).
 * - **Cancellable**: supports `.dismiss()` if the deferred rollback action is no longer needed (e.g., after a commit).
 */

#include <concepts>
#include <type_traits>
#include <utility>

namespace gorun {

/**
 * @class DeferGuard
 * @brief RAII scope guard that invokes a callable `F` upon leaving the current lexical scope.
 *
 * @tparam F Nullary callable type (`void()`).
 */
template <std::invocable F>
class [[nodiscard("gorun::defer(...) returns an RAII guard; store it in a local variable (auto _ = gorun::defer(...)) or use GORUN_DEFER")]] DeferGuard {
private:
    F fn_;
    bool active_{true};

public:
    /**
     * @brief Constructs an active `DeferGuard` taking ownership of `fn`.
     */
    constexpr explicit DeferGuard(F&& fn) noexcept(std::is_nothrow_move_constructible_v<F>)
        : fn_(std::move(fn))
        , active_(true) {}

    constexpr explicit DeferGuard(const F& fn) noexcept(std::is_nothrow_copy_constructible_v<F>)
        : fn_(fn)
        , active_(true) {}

    /**
     * @brief Move constructor transfers ownership of the deferred action and deactivates `other`.
     */
    constexpr DeferGuard(DeferGuard&& other) noexcept(std::is_nothrow_move_constructible_v<F>)
        : fn_(std::move(other.fn_))
        , active_(std::exchange(other.active_, false)) {}

    DeferGuard(const DeferGuard&) = delete;
    DeferGuard& operator=(const DeferGuard&) = delete;
    DeferGuard& operator=(DeferGuard&&) = delete;

    /**
     * @brief Executes the deferred callable on scope exit if still active.
     */
    ~DeferGuard() noexcept {
        if (active_) {
            try {
                fn_();
            } catch (...) {
                // Never allow an exception to escape a destructor during stack unwinding
            }
        }
    }

    /**
     * @brief Cancels the deferred action so that it will not run when the scope exits.
     */
    constexpr void dismiss() noexcept {
        active_ = false;
    }

    /**
     * @brief Returns `true` if the deferred action is still armed to run on scope exit.
     */
    [[nodiscard]] constexpr bool is_active() const noexcept {
        return active_;
    }
};

/**
 * @brief Creates a `DeferGuard` that executes `fn()` when the current lexical scope exits.
 *
 * @code
 * gorun::WaitGroup wg;
 * wg.add(1);
 * gorun::go([&]() {
 *     auto _ = gorun::defer([&]() { wg.done(); });
 *     // ... do work (even if an early return or exception occurs, wg.done() is guaranteed to run!)
 * });
 * @endcode
 *
 * @tparam F Nullary callable type.
 * @param fn Function or lambda to execute on scope exit.
 * @return `DeferGuard<std::decay_t<F>>`
 */
template <std::invocable F>
[[nodiscard]] constexpr auto defer(F&& fn) noexcept(std::is_nothrow_constructible_v<std::decay_t<F>, F>) {
    return DeferGuard<std::decay_t<F>>(std::forward<F>(fn));
}

namespace detail {

/**
 * @struct DeferOperatorHelper
 * @brief Helper tag enabling statement-style `GORUN_DEFER { ... };` syntax without parentheses.
 */
struct DeferOperatorHelper {
    template <std::invocable F>
    constexpr auto operator+(F&& fn) const noexcept(std::is_nothrow_constructible_v<std::decay_t<F>, F>) {
        return DeferGuard<std::decay_t<F>>(std::forward<F>(fn));
    }
};

} // namespace detail

} // namespace gorun

#define GORUN_DETAIL_CONCAT_IMPL(a, b) a##b
#define GORUN_DETAIL_CONCAT(a, b) GORUN_DETAIL_CONCAT_IMPL(a, b)

/**
 * @def GORUN_DEFER
 * @brief Ergonomic statement-style macro for Go-like `defer` blocks without naming a dummy variable.
 *
 * Automatically generates a unique hidden stack variable using `__COUNTER__` and captures the block by reference `[&]`.
 *
 * @code
 * gorun::Channel<int> ch(4);
 * gorun::go([&]() {
 *     GORUN_DEFER { ch.close(); };
 *     ch.send(10);
 *     ch.send(20);
 * });
 * @endcode
 */
#define GORUN_DEFER \
    [[maybe_unused]] const auto GORUN_DETAIL_CONCAT(_gorun_defer_guard_, __COUNTER__) = ::gorun::detail::DeferOperatorHelper{} + [&]()
