#pragma once

/**
 * @file context_go.hpp
 * @brief Go-style cascading request context (`gorun::Context`, `with_cancel`, `with_timeout`,
 *        `with_deadline`, `with_value`), modeled directly on Go's standard `context` package.
 *
 * Hardened for high-throughput web servers:
 * - Zero leaks & zero global lock contention on `background()` / `todo()` (`cancellable == false`).
 * - O(1) detachment of canceled/destroyed child contexts from parent trees (zero dead `weak_ptr` accumulation).
 * - Immediate timer cancellation on `cancel()` / destruction (zero dead timers left in `Runtime::timers_`).
 * - Read-only `DoneChannelView` returned by `ctx.done()` so consumers cannot forge cancellation or corrupt `background()`.
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
#include <unordered_map>
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

[[nodiscard]] constexpr std::string_view to_string(ContextError err) noexcept {
    switch (err) {
        case ContextError::None:             return "";
        case ContextError::Canceled:         return "context canceled";
        case ContextError::DeadlineExceeded: return "context deadline exceeded";
    }
    return "unknown context error";
}

using CancelFunc = std::function<void()>;

/**
 * @class DoneChannelView
 * @brief Read-only view over a `Context`'s broadcast cancellation channel (`<-chan struct{}` in Go).
 *
 * Prevents external callers from forging cancellation (`close()`) or sending bogus values into a shared `Context`.
 */
class DoneChannelView {
    Channel<bool>* ch_;

public:
    explicit DoneChannelView(Channel<bool>& ch) noexcept : ch_(&ch) {}

    [[nodiscard]] std::optional<bool> recv() const {
        return ch_->recv();
    }

    [[nodiscard]] bool is_closed() const noexcept {
        return ch_->is_closed();
    }

    template <typename Callback>
    auto on_recv(Callback&& cb) const {
        return ch_->on_recv(std::forward<Callback>(cb));
    }
};

namespace detail {

struct ContextNode : std::enable_shared_from_this<ContextNode> {
    std::shared_ptr<ContextNode> parent{nullptr};
    bool cancellable{true};

    // Immutable key-value pair attached to this node (if created via with_value)
    std::optional<std::string> key{};
    std::any value{};

    // Optional deadline (clamped to parent's deadline if parent has an earlier one)
    std::optional<std::chrono::steady_clock::time_point> deadline{};

    // Cancellable runtime timer handle (reclaimed immediately on cancel() or ~ContextNode())
    Runtime* timer_runtime{nullptr};
    std::uint64_t timer_id{0};

    // Synchronization for cancellation and child propagation
    SpinLock lock_{};
    std::atomic<ContextError> err_{ContextError::None};
    std::stop_source stop_source_{};
    Channel<bool> done_channel_{0};
    std::unordered_map<ContextNode*, std::weak_ptr<ContextNode>> children_{};

    explicit ContextNode(bool is_cancellable = true) : cancellable(is_cancellable) {}

    ~ContextNode() noexcept {
        cancel_pending_timer();
        detach_from_parent();
    }

    void cancel_pending_timer() noexcept {
        Runtime* rt = nullptr;
        std::uint64_t tid = 0;
        {
            std::lock_guard guard(lock_);
            rt = std::exchange(timer_runtime, nullptr);
            tid = std::exchange(timer_id, 0);
        }
        if (rt && tid != 0) {
            rt->cancel_timer(tid);
        }
    }

    void detach_from_parent() noexcept {
        if (parent && parent->cancellable) {
            std::lock_guard guard(parent->lock_);
            parent->children_.erase(this);
        }
    }

    void cancel_with_reason(ContextError reason) noexcept {
        if (!cancellable) {
            return;
        }
        std::unordered_map<ContextNode*, std::weak_ptr<ContextNode>> children_to_cancel;
        Runtime* rt = nullptr;
        std::uint64_t tid = 0;
        {
            std::lock_guard guard(lock_);
            if (err_.load(std::memory_order_relaxed) != ContextError::None) {
                return; // Already canceled
            }
            err_.store(reason, std::memory_order_release);
            children_to_cancel = std::move(children_);
            rt = std::exchange(timer_runtime, nullptr);
            tid = std::exchange(timer_id, 0);
        }

        // 0. Cancel pending deadline timer & detach from parent so parent never leaks dead children!
        if (rt && tid != 0) {
            rt->cancel_timer(tid);
        }
        detach_from_parent();

        // 1. Signal C++20 cooperative stop_token
        stop_source_.request_stop();

        // 2. Close the done_channel_ (wakes all routines parked on ctx.done().recv() or select(ctx.on_done(...)))
        done_channel_.close();

        // 3. Cascade cancellation down to all child contexts
        for (auto& [raw_ptr, weak_child] : children_to_cancel) {
            if (auto child = weak_child.lock()) {
                child->cancel_with_reason(reason);
            }
        }
    }

    static void attach_to_parent(const std::shared_ptr<ContextNode>& parent_node,
                                 const std::shared_ptr<ContextNode>& child_node) {
        // Non-cancellable root contexts (background(), todo()) NEVER lock or retain child pointers!
        if (!parent_node || !parent_node->cancellable) {
            return;
        }

        ContextError parent_err = ContextError::None;
        {
            std::lock_guard guard(parent_node->lock_);
            parent_err = parent_node->err_.load(std::memory_order_acquire);
            if (parent_err == ContextError::None) {
                parent_node->children_.emplace(child_node.get(), child_node);
                return;
            }
        }
        child_node->cancel_with_reason(parent_err);
    }

    [[nodiscard]] std::size_t child_count() noexcept {
        std::lock_guard guard(lock_);
        return children_.size();
    }
};

} // namespace detail

/**
 * @class Context
 * @brief Lightweight, copyable handle to a scoped request context (direct equivalent of `context.Context` in Go).
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
    Context() : node_(std::make_shared<detail::ContextNode>(false)) {}

    [[nodiscard]] bool can_cancel() const noexcept {
        return node_ && node_->cancellable;
    }

    [[nodiscard]] bool is_canceled() const noexcept {
        return node_->err_.load(std::memory_order_acquire) != ContextError::None;
    }

    [[nodiscard]] std::optional<ContextError> err() const noexcept {
        ContextError e = node_->err_.load(std::memory_order_acquire);
        if (e == ContextError::None) {
            return std::nullopt;
        }
        return e;
    }

    [[nodiscard]] std::optional<std::chrono::steady_clock::time_point> deadline() const noexcept {
        return node_->deadline;
    }

    /**
     * @brief Returns a read-only broadcast view (`DoneChannelView`) that closes when this context is canceled.
     */
    [[nodiscard]] DoneChannelView done() const noexcept {
        return DoneChannelView{node_->done_channel_};
    }

    void wait_done() const noexcept {
        (void)node_->done_channel_.recv();
    }

    template <typename Callback>
    auto on_done(Callback&& cb) const {
        return node_->done_channel_.on_recv([callback = std::forward<Callback>(cb)](std::optional<bool>) mutable {
            callback();
        });
    }

    [[nodiscard]] std::stop_token get_stop_token() const noexcept {
        return node_->stop_source_.get_token();
    }

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

    // Diagnostic helper for verifying zero child retention leaks in tests
    [[nodiscard]] std::size_t active_children_count() const noexcept {
        return node_->child_count();
    }
};

[[nodiscard]] inline Context background() {
    static Context root_ctx{std::make_shared<detail::ContextNode>(false)};
    return root_ctx;
}

[[nodiscard]] inline Context todo() {
    static Context todo_ctx{std::make_shared<detail::ContextNode>(false)};
    return todo_ctx;
}

[[nodiscard]] inline std::pair<Context, CancelFunc> with_cancel(const Context& parent) {
    auto child_node = std::make_shared<detail::ContextNode>(true);
    child_node->parent = parent.node_;
    child_node->deadline = parent.node_->deadline;

    detail::ContextNode::attach_to_parent(parent.node_, child_node);

    CancelFunc cancel_fn = [child_node]() noexcept {
        child_node->cancel_with_reason(ContextError::Canceled);
    };

    return {Context{std::move(child_node)}, std::move(cancel_fn)};
}

[[nodiscard]] inline std::pair<Context, CancelFunc> with_deadline(
    const Context& parent,
    std::chrono::steady_clock::time_point deadline,
    Runtime& rt = default_runtime()
) {
    auto child_node = std::make_shared<detail::ContextNode>(true);
    child_node->parent = parent.node_;

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
        std::uint64_t tid = rt.schedule_timer_callback(effective_deadline, [weak_child]() {
            if (auto locked = weak_child.lock()) {
                locked->cancel_with_reason(ContextError::DeadlineExceeded);
            }
        });
        {
            std::lock_guard guard(child_node->lock_);
            if (child_node->err_.load(std::memory_order_relaxed) == ContextError::None) {
                child_node->timer_runtime = &rt;
                child_node->timer_id = tid;
            } else {
                // Canceled concurrently before timer_id was stored -> immediately cancel timer!
                rt.cancel_timer(tid);
            }
        }
    }

    CancelFunc cancel_fn = [child_node]() noexcept {
        child_node->cancel_with_reason(ContextError::Canceled);
    };

    return {Context{std::move(child_node)}, std::move(cancel_fn)};
}

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

template <typename T>
[[nodiscard]] Context with_value(const Context& parent, std::string key, T&& val) {
    // Inherit cancellability from parent: with_value(background(), ...) stays non-cancellable (0 locks, 0 tracking!)
    auto child_node = std::make_shared<detail::ContextNode>(parent.node_->cancellable);
    child_node->parent = parent.node_;
    child_node->deadline = parent.node_->deadline;
    child_node->key = std::move(key);
    child_node->value = std::decay_t<T>(std::forward<T>(val));

    detail::ContextNode::attach_to_parent(parent.node_, child_node);
    return Context{std::move(child_node)};
}

} // namespace gorun
