#pragma once

/**
 * @file gorun.hpp
 * @brief Umbrella header for the `gorun` C++20/23 M:N Green Threads, Channels & Structured Concurrency Runtime.
 *
 * Include this single header to access the entire `gorun` API:
 * - Launching: `gorun::go(...)`, `gorun::go_in(...)`, `gorun::go_async(...)`
 * - Channels: `gorun::Channel<T>`, `gorun::select(...)`, `gorun::on_timeout(...)`, `gorun::on_default(...)`
 * - Structured Concurrency: `gorun::Scope`, `gorun::TaskGroup`, `gorun::JoinHandle<T>`
 * - Green Synchronization: `gorun::Mutex`, `gorun::CondVar`, `gorun::WaitGroup`, `gorun::Semaphore`, `gorun::Once`, `gorun::RwMutex`
 * - Scope Cleanup: `gorun::defer(...)`, `GORUN_DEFER { ... };`
 * - Routine Control: `gorun::yield()`, `gorun::sleep_for(...)`, `gorun::this_routine::id()`, `gorun::RoutineLocal<T>`
 */

#include "gorun/detail/spinlock.hpp"
#include "gorun/detail/stack_pool.hpp"
#include "gorun/detail/context.hpp"
#include "gorun/detail/work_stealing_deque.hpp"
#include "gorun/defer.hpp"
#include "gorun/runtime.hpp"
#include "gorun/sync.hpp"
#include "gorun/channel.hpp"
#include "gorun/task_group.hpp"
