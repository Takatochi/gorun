# gorun v0.3.0-alpha

Security & concurrency hardening release introducing a zero-allocation single-header distribution, true parked multi-channel `select()`, full FPU/MXCSR context switching, move-only lambda support, and a live C++20 browser playground styled in IntelliJ IDEA / CLion Darcula.

## Security, Memory-Safety & Concurrency Hardening

- **Full Floating-Point Context Preservation (`detail/context.hpp`):**
  `gorun_context_switch_asm` now saves and restores SSE `MXCSR` (`stmxcsr`/`ldmxcsr`) and the x87 FPU control word (`fnstcw`/`fldcw`) on both Windows x64 and Linux System V AMD64, with `.cfi_startproc`/`.cfi_endproc` unwind directives on Linux and explicit compile-time guards against unsupported compilers/architectures.
- **Stack Allocator Hardening (`detail/stack_pool.hpp`):**
  Validates `sysconf(_SC_PAGESIZE)` with a safe 4096-byte fallback, enforces a 16 KB minimum stack size, and prevents unsigned integer wraparound when aligning stack sizes with the hardware Guard Page.
- **True Parked Multi-Channel `gorun::select(...)` (`channel.hpp`):**
  Replaced the previous busy-spin `yield()` loop with a deadlock-free 2-phase parked multiplexer using address-sorted `SpinLock*` acquisition, stack-allocated `ChannelWaiter` unlinking, cancellable `on_timeout` timers, and proper closed-channel wakeups for `on_recv` and `on_send`.
- **Channel Exception Safety & Zero-Alloc `close()` (`channel.hpp`):**
  Wrapped `send()`/`recv()` critical sections in RAII `std::unique_lock<SpinLock>` with waiter rollback if a payload's move constructor throws, and replaced `std::vector` heap allocation in `close() noexcept` with O(1) `IntrusiveQueue` draining.
- **Zero-Leak `gorun::Context` Tree & Cancellable Timers (`context_go.hpp`, `runtime.hpp`):**
  - Replaced `std::priority_queue` timers with cancellable `std::multimap` + `cancel_timer(id)` so cancelled `with_timeout` / `on_timeout` timers are reclaimed immediately.
  - Made `background()` / `todo()` non-cancellable (eliminating unbounded child accumulation and global spinlock contention).
  - Switched child tracking to `std::unordered_map<ContextNode*, std::weak_ptr<ContextNode>>` with O(1) parent detachment in `cancel()` and `~ContextNode()`.
  - Changed `ctx.done()` to return a read-only `DoneChannelView` to prevent external cancellation forgery.
- **Lock-Free, Zero-Leak `RoutineLocal<T>` (`runtime.hpp`):**
  Moved per-routine storage directly onto `Routine::routine_locals` with deterministic destruction (`r->clear_locals()`) before stack recycling, plus an isolated `thread_local` fallback for external OS threads.
- **C++20 Move-Only `UniqueTask` with 32-Byte SBO (`runtime.hpp`):**
  Replaced `std::function<void()>` so `gorun::go()`, `TaskGroup::spawn()`, and `gorun::go_async()` natively accept move-only lambdas capturing `std::unique_ptr<T>` with zero heap allocation for typical closures.
- **Scheduler Fairness, Lost-Wakeup & Nested `block_on()` Fixes (`runtime.hpp`, `work_stealing_deque.hpp`, `task_group.hpp`, `sync.hpp`):**
  - `yield()` now enqueues onto the FIFO head (`push_front`) to eliminate LIFO self-starvation.
  - Added `std::atomic_thread_fence(seq_cst)` + `waking_workers_` coordination to prevent Store-Load lost wakeups without degrading L1 hot-slot throughput (`78.00 M ops/s` on buffered channels).
  - Rewrote `Runtime::block_on()` using `UniversalWaiter` so nested `block_on()` inside a green routine on a 1-worker runtime never deadlocks.
  - Removed `reinterpret_cast<Scope*>` UB in `TaskGroup`, added exception-unwinding auto-`cancel()` in `~TaskGroup()`, and added underflow/invalid-unlock guards in `Mutex`, `RwMutex`, `WaitGroup`, and `Semaphore`.

## Single-Header Distribution

- Added `scripts/amalgamate.py` generating a standalone drop-in single header:
  - `single_include/gorun/gorun.hpp` (11 headers inlined, zero external dependencies)
  - Included directly as a downloadable release asset (`gorun-v0.3.0-alpha-single-header.hpp`)

## Interactive Documentation & Live C++20 Playground

- Redesigned the GitHub Pages documentation site (`https://takatochi.github.io/gorun/`) with:
  - Custom generated brand emblem (`docs/logo.png` and `docs/logo.svg`).
  - **IntelliJ IDEA / CLion Darcula** syntax highlighting via CodeMirror 5 (`Ctrl+Enter` shortcut, bracket matching, active line highlighting) plus Darcula highlighting for static code examples.
  - **Real Live C++20 Compilation & Execution** powered by Compiler Explorer (GCC 14.2.0 on Linux x86_64) bundling `gorun.hpp` automatically — users can edit any of the 5 official examples or write arbitrary custom C++20 code in **06. Custom Sandbox**.

## Verification

- Expanded the test suite from 21 to **25 comprehensive unit, race-condition, leak-audit, and security regression tests** (`[TEST 01]` through `[TEST 25]`).
- Verified **0 compiler warnings** under `-O3 -Wall -Wextra -Wpedantic` across all modular and single-header targets.
- Verified **25/25 tests passing** on both modular headers and `single_include/gorun/gorun.hpp`.

## Release Assets

- `gorun-v0.3.0-alpha-single-header.hpp` — Single drop-in header (`#include "gorun.hpp"`).
- `gorun-v0.3.0-alpha-header-only.tar.gz` / `.zip` — Full modular headers, `single_include/`, and CMake package files.
