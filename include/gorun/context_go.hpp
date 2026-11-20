#pragma once

/**
 * @file context_go.hpp
 * @brief Go-style cascading request context (`gorun::Context`, `with_cancel`, `with_timeout`,
 *        `with_deadline`, `with_value`), modeled directly on Go's standard `context` package.
 *
 * Essential foundation for building HTTP web frameworks, RPC microservices, and database drivers on top of `gorun`:
 * - **Cascading Cancellation Tree:** Cancelling a parent `Context` atomically cancels all child and descendant
 *   contexts in the tree and closes their `done()` channels (`Channel<bool>`), immediately waking any `gorun::select(...)`
 *   clauses waiting on `ctx.on_done(...)` or `ctx.done().on_recv(...)`!
 * - **Deadline & Timeout Propagation:** `with_timeout(parent, 50ms)` and `with_deadline(parent, tp)` automatically
 *   clamp the child's deadline to the earliest ancestor deadline and schedule a non-OS-blocking timer on the runtime.
 * - **Immutable Key-Value Request Bag (`with_value`):** Thread-safe hierarchical lookup for request-scoped metadata
 *   (Trace ID, User Session, Auth Claims, Logger) with zero locks on read.
 */

#include <any>
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "gorun/channel.hpp"
#include "gorun/runtime.hpp"

namespace gorun {

/**
 * @enum ContextError
 * @brief Standard cancellation reasons modeled on Go's `context.Canceled` and `context.DeadlineExceeded`.
 */
enum class ContextError : std::uint8_t {
    None = 0,          ///< Context is still active (`ctx.err() == std::nullopt`)
    Canceled,          ///< Explicitly canceled via `CancelFunc` or parent cancellation (`context.Canceled`)
    DeadlineExceeded   ///< Expired due to timeout or deadline (`context.DeadlineExceeded`)
};

/**
 * @brief Converts a `ContextError` to a human-readable string matching Go's `context` error messages.
 */
[[nodiscard]] constexpr std::string_view to_string(ContextError err) noexcept {
    switch (err) {
        case ContextError::None:             return "";
        case ContextError::Canceled:         return "context canceled";
        case ContextError::DeadlineExceeded: return "context deadline exceeded";
    }
    return "unknown context error";
}

/**
 * @typedef CancelFunc
 * @brief Callable returned by `with_cancel`, `with_timeout`, and `with_deadline` to cancel a `Context` subtree.
 *
 * Safe to call multiple times from multiple green routines concurrently (idempotent).
 * Very commonly paired with `GORUN_DEFER { cancel(); };` just like `defer cancel()` in Go.
 */
using CancelFunc = std::function<void()>;

namespace detail {

struct ContextNode : std::enable_shared_from_this<ContextNode> {
    std::shared_ptr<ContextNode> parent{nullptr};

    // Immutable key-value pair attached to this node (if created via with_value)
    std::optional<std::string> key{};
    std::any value{};

    // Optional deadline (clamped to parent's deadline if parent has an earlier one)
    std::optional<std::chrono::steady_clock::time_point> deadline{};

    // Synchronization for cancellation and child propagation
    SpinLock lock_{};
    std::atomic<ContextError> err_{ContextError::None};
    std::stop_source stop_source_{};
    Channel<bool> done_channel_{0}; // Closed upon cancellation to broadcast-wake all receivers & select()!
    std::vector<std::weak_ptr<ContextNode>> children_{};

    ContextNode() = default;

    void cancel_with_reason(ContextError reason) noexcept {
        std::vector<std::weak_ptr<ContextNode>> children_to_cancel;
        {
            std::lock_guard guard(lock_);
            if (err_.load(std::memory_order_relaxed) != ContextError::None) {
                return; // Already canceled
            }
            err_.store(reason, std::memory_order_release);
            children_to_cancel = std::move(children_);
        }

        // 1. Signal C++20 cooperative stop_token
        stop_source_.request_stop();

        // 2. Close the done_channel_ (wakes all routines parked on ctx.done().recv() or select(ctx.on_done(...)))
        done_channel_.close();

        // 3. Cascade cancellation down to all child contexts
        for (auto& weak_child : children_to_cancel) {
            if (auto child = weak_child.lock()) {
                child->cancel_with_reason(reason);
            }
        }
    }

    static void attach_to_parent(const std::shared_ptr<ContextNode>& parent_node,
                                 const std::shared_ptr<ContextNode>& child_node) {
        if (!parent_node) return;

        ContextError parent_err = ContextError::None;
        {
            std::lock_guard guard(parent_node->lock_);
            parent_err = parent_node->err_.load(std::memory_order_acquire);
            if (parent_err == ContextError::None) {
                parent_node->children_.push_back(child_node);
                return;
            }
        }
        // Parent was already canceled before we attached — immediately cancel the child with the same reason
        child_node->cancel_with_reason(parent_err);
    }
};

} // namespace detail

/**
 * @class Context
 * @brief Lightweight, copyable handle to a scoped request context (direct equivalent of `context.Context` in Go).
 *
 * A `Context` carries deadlines, cancellation signals, and request-scoped key-value pairs across
 * API boundaries and between green routines. Copying a `Context` is cheap (copies a single `std::shared_ptr`).
 *
 * @code
 * auto [ctx, cancel] = gorun::with_timeout(gorun::background(), 50ms);
 * GORUN_DEFER { cancel(); };
 *
 * gorun::select(
 *     work_ch.on_recv([](int res) { std::cout << "Result: " << res << "\n"; }),
 *     ctx.on_done([&]() { std::cout << "Canceled: " << gorun::to_string(*ctx.err()) << "\n"; })
 * );
 * @endcode
 */
class Context {
private:
    std::shared_ptr<detail::ContextNode> node_;

    explicit Context(std::shared_ptr<detail::ContextNode> node) noexcept
        : node_(std::move(node)) {}

    friend Context background();
    friend Context todo();
    friend std::pair<Context, CancelFunc> with_cancel(const Context& parent);
    friend std::pair<Context, CancelFunc> with_deadline(
        const Context& parent,
        std::chrono::steady_clock::time_point deadline,
        Runtime& rt
    );
    template <typename T>
    friend Context with_value(const Context& parent, std::string key, T&& val);

public:
    Context() : node_(std::make_shared<detail::ContextNode>()) {}

    /**
     * @brief Returns `true` if this context has been canceled or its deadline has expired.
     */
    [[nodiscard]] bool is_canceled() const noexcept {
        return node_->err_.load(std::memory_order_acquire) != ContextError::None;
    }

    /**
     * @brief Returns the cancellation reason (`ContextError::Canceled` or `ContextError::DeadlineExceeded`),
     *        or `std::nullopt` if the context is still active (analogous to `ctx.Err()` in Go).
     */
    [[nodiscard]] std::optional<ContextError> err() const noexcept {
        ContextError e = node_->err_.load(std::memory_order_acquire);
        if (e == ContextError::None) {
            return std::nullopt;
        }
        return e;
    }

    /**
     * @brief Returns the deadline time point if one is associated with this context, or `std::nullopt`
     *        (analogous to `deadline, ok := ctx.Deadline()` in Go).
     */
    [[nodiscard]] std::optional<std::chrono::steady_clock::time_point> deadline() const noexcept {
        return node_->deadline;
    }

    /**
     * @brief Returns a reference to the broadcast channel that is closed when this context is canceled
     *        (analogous to `<-ctx.Done()` in Go).
     *
     * Because closing a `gorun::Channel` wakes all waiting receivers with `std::nullopt`, multiple
     * green routines can wait on `ctx.done().recv()` or `ctx.on_done(...)` simultaneously!
     */
    [[nodiscard]] Channel<bool>& done() const noexcept {
        return node_->done_channel_;
    }

    /**
     * @brief Blocks (parks the current green routine) until this context is canceled or its deadline expires.
     */
    void wait_done() const noexcept {
        (void)node_->done_channel_.recv();
    }

    /**
     * @brief Convenience helper for `gorun::select(...)` that triggers `cb()` as soon as this context is canceled
     *        (direct equivalent of `case <-ctx.Done():` in Go!).
     *
     * @param cb Nullary callback `void()` invoked when the context is canceled or times out.
     */
    template <typename Callback>
    auto on_done(Callback&& cb) const {
        return node_->done_channel_.on_recv([callback = std::forward<Callback>(cb)](std::optional<bool>) mutable {
            callback();
        });
    }

    /**
     * @brief Returns the C++20 `std::stop_token` linked to this context's cancellation state.
     */
    [[nodiscard]] std::stop_token get_stop_token() const noexcept {
        return node_->stop_source_.get_token();
    }

    /**
     * @brief Looks up a request-scoped value of type `T` associated with `key` in this context or its ancestors
     *        (analogous to `ctx.Value(key)` in Go, but type-safe via `std::optional<T>`).
     *
     * Traverses the immutable parent chain lock-free in O(depth) time.
     *
     * @tparam T Expected value type.
     * @param key String key to search for.
     * @return `std::optional<T>` containing the value if found and of matching type `T`, or `std::nullopt`.
     */
    template <typename T>
    [[nodiscard]] std::optional<T> value(std::string_view key) const {
        for (auto cur = node_; cur != nullptr; cur = cur->parent) {
            if (cur->key.has_value() && *cur->key == key && cur->value.has_value()) {
                if (const T* ptr = std::any_cast<T>(&cur->value)) {
                    return *ptr;
                }
            }
        }
        return std::nullopt;
    }
};

/**
 * @brief Returns a non-null, empty root `Context` that is never canceled, has no values, and has no deadline
 *        (direct equivalent of `context.Background()` in Go).
 *
 * Typically used by `main()`, initialization, and top-level incoming HTTP request handlers.
 */
[[nodiscard]] inline Context background() {
    static Context root_ctx{std::make_shared<detail::ContextNode>()};
    return root_ctx;
}

/**
 * @brief Returns a non-null, empty root `Context` for code where it is unclear which Context to use yet
 *        (direct equivalent of `context.TODO()` in Go).
 */
[[nodiscard]] inline Context todo() {
    static Context todo_ctx{std::make_shared<detail::ContextNode>()};
    return todo_ctx;
}

/**
 * @brief Creates a child `Context` and a `CancelFunc` (direct equivalent of `context.WithCancel(parent)` in Go).
 *
 * Cancelling the returned `CancelFunc` (or cancelling `parent`) closes `child.done()` and cascades
 * cancellation to all descendants of `child`.
 *
 * @param parent Parent context to inherit values, deadlines, and cancellation from.
 * @return `std::pair<Context, CancelFunc>` (use structured bindings: `auto [ctx, cancel] = gorun::with_cancel(parent);`).
 */
[[nodiscard]] inline std::pair<Context, CancelFunc> with_cancel(const Context& parent) {
    auto child_node = std::make_shared<detail::ContextNode>();
    child_node->parent = parent.node_;
    child_node->deadline = parent.node_->deadline;

    detail::ContextNode::attach_to_parent(parent.node_, child_node);

    CancelFunc cancel_fn = [child_node]() noexcept {
        child_node->cancel_with_reason(ContextError::Canceled);
    };

    return {Context{std::move(child_node)}, std::move(cancel_fn)};
}

/**
 * @brief Creates a child `Context` that automatically cancels at `deadline` with `ContextError::DeadlineExceeded`
 *        (direct equivalent of `context.WithDeadline(parent, d)` in Go).
 *
 * If `parent` already has an earlier deadline, the child's effective deadline is clamped to the parent's deadline.
 *
 * @param parent Parent context.
 * @param deadline Absolute `steady_clock::time_point` when the context should expire.
 * @param rt Runtime whose timer thread will trigger the deadline expiration (defaults to `default_runtime()`).
 * @return `std::pair<Context, CancelFunc>`
 */
[[nodiscard]] inline std::pair<Context, CancelFunc> with_deadline(
    const Context& parent,
    std::chrono::steady_clock::time_point deadline,
    Runtime& rt = default_runtime()
) {
    auto child_node = std::make_shared<detail::ContextNode>();
    child_node->parent = parent.node_;

    // Clamp to parent's deadline if parent expires earlier
    if (parent.node_->deadline.has_value() && *parent.node_->deadline < deadline) {
        child_node->deadline = parent.node_->deadline;
    } else {
        child_node->deadline = deadline;
    }

    detail::ContextNode::attach_to_parent(parent.node_, child_node);

    auto effective_deadline = *child_node->deadline;
    if (std::chrono::steady_clock::now() >= effective_deadline) {
        child_node->cancel_with_reason(ContextError::DeadlineExceeded);
    } else if (child_node->err_.load(std::memory_order_acquire) == ContextError::None) {
        std::weak_ptr<detail::ContextNode> weak_child = child_node;
        rt.schedule_timer_callback(effective_deadline, [weak_child]() {
            if (auto locked = weak_child.lock()) {
                locked->cancel_with_reason(ContextError::DeadlineExceeded);
            }
        });
    }

    CancelFunc cancel_fn = [child_node]() noexcept {
        child_node->cancel_with_reason(ContextError::Canceled);
    };

    return {Context{std::move(child_node)}, std::move(cancel_fn)};
}

/**
 * @brief Creates a child `Context` that automatically cancels after `timeout` with `ContextError::DeadlineExceeded`
 *        (direct equivalent of `context.WithTimeout(parent, timeout)` in Go).
 *
 * @code
 * auto [ctx, cancel] = gorun::with_timeout(gorun::background(), 100ms);
 * GORUN_DEFER { cancel(); };
 * @endcode
 *
 * @param parent Parent context.
 * @param timeout Relative duration before the context expires.
 * @param rt Runtime whose timer thread will trigger the timeout (defaults to `default_runtime()`).
 * @return `std::pair<Context, CancelFunc>`
 */
template <typename Rep, typename Period>
[[nodiscard]] std::pair<Context, CancelFunc> with_timeout(
    const Context& parent,
    std::chrono::duration<Rep, Period> timeout,
    Runtime& rt = default_runtime()
) {
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::duration_cast<std::chrono::steady_clock::duration>(timeout);
    return with_deadline(parent, deadline, rt);
}

/**
 * @brief Creates a child `Context` carrying an immutable request-scoped `(key, val)` pair
 *        (direct equivalent of `context.WithValue(parent, key, val)` in Go).
 *
 * Ideal for passing HTTP request metadata (Trace ID, Auth User ID, Request Logger) across green routines.
 *
 * @code
 * auto ctx = gorun::with_value(gorun::background(), "trace_id", std::string("req-9941"));
 * auto trace = ctx.value<std::string>("trace_id"); // *trace == "req-9941"
 * @endcode
 *
 * @tparam T Value type to store.
 * @param parent Parent context.
 * @param key String key identifier.
 * @param val Value to associate with `key`.
 * @return New child `Context` inheriting `parent`'s cancellation, deadlines, and ancestor values.
 */
template <typename T>
[[nodiscard]] Context with_value(const Context& parent, std::string key, T&& val) {
    auto child_node = std::make_shared<detail::ContextNode>();
    child_node->parent = parent.node_;
    child_node->deadline = parent.node_->deadline;
    child_node->key = std::move(key);
    child_node->value = std::decay_t<T>(std::forward<T>(val));

    detail::ContextNode::attach_to_parent(parent.node_, child_node);
    return Context{std::move(child_node)};
}

} // namespace gorun
