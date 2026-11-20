# Changelog

All notable changes to `gorun` are documented in this file.

## [v0.3.0-alpha] - 2026-11-21

### Security & Concurrency Hardening
- Saved and restored `MXCSR` and x87 FPU control word across `gorun_context_switch_asm` on Windows x64 and Linux System V AMD64; added `.cfi_startproc`/`.cfi_endproc` directives and explicit architecture/compiler guards.
- Hardened `StackPool` against invalid `sysconf(_SC_PAGESIZE)` values, undersized stacks (< 16 KB), and unsigned integer overflow.
- Replaced busy-spin `gorun::select(...)` with a true 2-phase parked multi-channel multiplexer with address-sorted `SpinLock*` locking and cancellable `on_timeout` timers.
- Added RAII `std::unique_lock<SpinLock>` exception safety and waiter rollback in `Channel<T>::send()`/`recv()`, and eliminated heap allocations inside `Channel<T>::close() noexcept`.
- Made `gorun::Context` timers cancellable (`Runtime::cancel_timer`), prevented child accumulation on `background()`/`todo()`, added O(1) parent detachment, and returned a read-only `DoneChannelView` from `ctx.done()`.
- Rewrote `RoutineLocal<T>` to store slots directly on `Routine` (lock-free and automatically destroyed on routine exit) with per-thread isolation for external OS threads.
- Replaced `std::function<void()>` with a C++20 move-only `UniqueTask` featuring 32-byte SBO (supports lambdas capturing `std::unique_ptr`).
- Fixed LIFO `yield()` self-starvation via `WorkStealingDeque::push_front()`, prevented Store-Load lost wakeups in `Runtime::schedule()`/`Worker::run_loop()`, fixed nested `Runtime::block_on()` deadlocks via `UniversalWaiter`, removed `TaskGroup`/`Scope` cast UB, and added invalid-unlock/underflow guards in `sync.hpp`.

### Added
- Single-header amalgamation generator (`scripts/amalgamate.py`) producing `single_include/gorun/gorun.hpp` and `docs/gorun.hpp`.
- 4 new regression test suites (`[TEST 22]`–`[TEST 25]`), bringing total coverage to 25 test suites.
- Interactive documentation website (`docs/index.html`) featuring brand artwork (`docs/logo.png`, `docs/logo.svg`), IntelliJ IDEA / CLion Darcula syntax highlighting, and live C++20 compilation/execution via GCC 14.2 (Compiler Explorer API).

## [v0.2.0-alpha] - 2026-11-20

### Added
- Go-style cascading `gorun::Context` (`background`, `todo`, `with_cancel`, `with_timeout`, `with_deadline`, `with_value`, `ctx.on_done`).
- `GORUN_DEFER` and `gorun::defer(...)` with LIFO ordering and `.dismiss()`.
- Expanded example suite (`examples/01..05`) including custom Linear Bump Arenas and raw stack pointer trees.
- 21 verification test suites and 3-way benchmarks (`gorun` vs Go 1.27 vs standard C++20).
- GitHub Pages documentation deployment workflow (`.github/workflows/pages.yml`).

## [v0.1.0-alpha] - 2026-11-20

### Added
- Initial header-only C++20/23 M:N green threads runtime (`Runtime`, `Worker`, `StackPool`, `WorkStealingDeque`, custom x86_64 context switching).
- Typed `Channel<T>` (buffered and unbuffered stack-to-stack rendezvous), `select(...)`, `Mutex`, `RwMutex`, `CondVar`, `WaitGroup`, `Semaphore`, `Once`, `Scope`, `TaskGroup`, `JoinHandle<T>`, and `RoutineLocal<T>`.
- CMake `gorun::gorun` export/install configuration and GitHub Actions CI matrix.
