#pragma once

/**
 * @file blocking.hpp
 * @brief Elastic OS-thread blocking pool (`gorun::BlockingPool`, `gorun::spawn_blocking`) analogous to
 *        `tokio::task::spawn_blocking`, plus non-blocking async file I/O helpers (`gorun::fs`).
 *
 * Offloads synchronous/blocking C libraries (SQLite, libpq, disk I/O, compression, crypto) onto
 * an elastic pool of OS threads while keeping all `gorun` M:N green-thread workers 100% unblocked.
 */

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <exception>
#include <fstream>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "gorun/task_group.hpp"

namespace gorun {

/**
 * @struct BlockingPoolConfig
 * @brief Configuration for the elastic `gorun::BlockingPool`.
 */
struct BlockingPoolConfig {
    std::size_t min_threads{2};                                  ///< Minimum warm OS threads kept alive
    std::size_t max_threads{64};                                 ///< Upper cap on elastic OS threads
    std::chrono::milliseconds idle_timeout{std::chrono::seconds(30)}; ///< Idle reclaim timeout for extra threads
};

/**
 * @class BlockingPool
 * @brief Elastic OS-thread pool for running blocking operations off `gorun`'s M:N scheduler workers.
 */
class BlockingPool {
    BlockingPoolConfig cfg_;
    mutable std::mutex mtx_;
    std::condition_variable cv_;
    std::deque<detail::UniqueTask> queue_;
    std::size_t total_threads_{0};
    std::size_t idle_threads_{0};
    bool stopping_{false};
    std::condition_variable exit_cv_;

    void spawn_worker_locked() {
        ++total_threads_;
        std::thread([this]() { worker_loop(); }).detach();
    }

    void worker_loop() {
        for (;;) {
            detail::UniqueTask task;
            {
                std::unique_lock lk(mtx_);
                ++idle_threads_;
                const bool extra_thread = total_threads_ > cfg_.min_threads;
                if (extra_thread) {
                    cv_.wait_for(lk, cfg_.idle_timeout, [this]() {
                        return stopping_ || !queue_.empty();
                    });
                } else {
                    cv_.wait(lk, [this]() {
                        return stopping_ || !queue_.empty();
                    });
                }
                --idle_threads_;

                if (queue_.empty()) {
                    if (stopping_ || total_threads_ > cfg_.min_threads) {
                        --total_threads_;
                        if (total_threads_ == 0) {
                            exit_cv_.notify_all();
                        }
                        return;
                    }
                    continue;
                }

                task = std::move(queue_.front());
                queue_.pop_front();
            }

            if (task) {
                task();
            }
        }
    }

public:
    explicit BlockingPool(BlockingPoolConfig cfg = {})
        : cfg_(cfg) {
        if (cfg_.max_threads == 0) cfg_.max_threads = 1;
        if (cfg_.min_threads > cfg_.max_threads) cfg_.min_threads = cfg_.max_threads;
    }

    ~BlockingPool() {
        shutdown();
    }

    BlockingPool(const BlockingPool&) = delete;
    BlockingPool& operator=(const BlockingPool&) = delete;

    void shutdown() noexcept {
        std::unique_lock lk(mtx_);
        if (stopping_) return;
        stopping_ = true;
        cv_.notify_all();
        exit_cv_.wait(lk, [this]() { return total_threads_ == 0; });
    }

    [[nodiscard]] std::size_t thread_count() const noexcept {
        std::lock_guard lk(mtx_);
        return total_threads_;
    }

    [[nodiscard]] std::size_t queued_count() const noexcept {
        std::lock_guard lk(mtx_);
        return queue_.size();
    }

    /**
     * @brief Submits a blocking callable `fn()` onto the elastic OS pool and returns a `JoinHandle<R>`.
     *        Calling `.get()` or `co_await` on the returned handle parks the green routine (zero worker blocking!).
     */
    template <std::invocable F>
    auto spawn(F&& fn) -> JoinHandle<std::invoke_result_t<F>> {
        using R = std::invoke_result_t<F>;
        using State = typename JoinHandle<R>::SharedState;
        auto state = std::make_shared<State>();
        state->wg.add(1);

        detail::UniqueTask job([state, f = std::forward<F>(fn)]() mutable {
            try {
                if constexpr (std::is_void_v<R>) {
                    std::invoke(f);
                } else {
                    state->value.emplace(std::invoke(f));
                }
            } catch (...) {
                state->exception = std::current_exception();
            }
            state->mark_ready();
        });

        {
            std::lock_guard lk(mtx_);
            if (stopping_) {
                state->exception = std::make_exception_ptr(std::runtime_error("BlockingPool is shutting down"));
                state->mark_ready();
                return JoinHandle<R>(std::move(state));
            }
            queue_.push_back(std::move(job));
            if (idle_threads_ == 0 && total_threads_ < cfg_.max_threads) {
                spawn_worker_locked();
            }
        }
        cv_.notify_one();
        return JoinHandle<R>(std::move(state));
    }
};

/// Returns the lazy global elastic `BlockingPool` used by `gorun::spawn_blocking(...)` and `gorun::fs`.
inline BlockingPool& default_blocking_pool() {
    static BlockingPool pool{};
    return pool;
}

/**
 * @brief Executes a blocking callable `fn()` on the elastic `BlockingPool` and returns a `JoinHandle<R>`
 *        (analogous to `tokio::task::spawn_blocking`).
 */
template <std::invocable F>
[[nodiscard]] auto spawn_blocking(F&& fn) -> JoinHandle<std::invoke_result_t<F>> {
    return default_blocking_pool().spawn(std::forward<F>(fn));
}

/**
 * @namespace gorun::fs
 * @brief Non-worker-blocking file I/O helpers built on `gorun::spawn_blocking` (parks calling green routine).
 */
namespace fs {

/// Reads an entire file into a `std::string` without blocking any `gorun` M:N worker thread.
inline std::string read_file(std::string path) {
    return spawn_blocking([p = std::move(path)]() -> std::string {
        std::ifstream ifs(p, std::ios::binary);
        if (!ifs) {
            throw std::runtime_error("gorun::fs::read_file failed to open: " + p);
        }
        std::ostringstream oss;
        oss << ifs.rdbuf();
        return oss.str();
    }).get();
}

/// Writes `content` to `path` without blocking any `gorun` M:N worker thread.
inline void write_file(std::string path, std::string content) {
    spawn_blocking([p = std::move(path), data = std::move(content)]() {
        std::ofstream ofs(p, std::ios::binary | std::ios::trunc);
        if (!ofs) {
            throw std::runtime_error("gorun::fs::write_file failed to open: " + p);
        }
        ofs.write(data.data(), static_cast<std::streamsize>(data.size()));
        if (!ofs) {
            throw std::runtime_error("gorun::fs::write_file failed writing: " + p);
        }
    }).get();
}

/// Appends `content` to `path` without blocking any `gorun` M:N worker thread.
inline void append_file(std::string path, std::string content) {
    spawn_blocking([p = std::move(path), data = std::move(content)]() {
        std::ofstream ofs(p, std::ios::binary | std::ios::app);
        if (!ofs) {
            throw std::runtime_error("gorun::fs::append_file failed to open: " + p);
        }
        ofs.write(data.data(), static_cast<std::streamsize>(data.size()));
        if (!ofs) {
            throw std::runtime_error("gorun::fs::append_file failed appending: " + p);
        }
    }).get();
}

} // namespace fs
} // namespace gorun
