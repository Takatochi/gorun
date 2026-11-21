# Changelog

All notable changes to `gorun` are documented in this file.

## [v0.4.0] - 2026-11-22

### Added
- **ARM64 (AArch64) Architecture Support (`include/gorun/detail/context.hpp`):** Added AAPCS64 assembly context switching (`x19`–`x30`, `sp`, SIMD `d8`–`d15`) with Mach-O symbol prefixes for Apple Silicon (macOS M1–M4) and Linux ARM64 servers (AWS Graviton).
- **Native Async Network Poller (`include/gorun/net.hpp`):** Built-in zero-dependency non-blocking TCP networking (`TcpListener`, `TcpStream`) over Windows IOCP (2 poller threads + `GetQueuedCompletionStatusEx` batching up to 64 completions/syscall + `FILE_SKIP_COMPLETION_PORT_ON_SUCCESS`), Linux `epoll`, and macOS/BSD `kqueue` with `gorun::Context` cancellation, zero-timer sync fast-path timeouts, and thread-safe `TcpStream::cancel_handle()`.
- **AddressSanitizer & ThreadSanitizer Fiber Support:** Integrated `__sanitizer_start_switch_fiber` / `__sanitizer_finish_switch_fiber` (ASan) and `__tsan_create_fiber` / `__tsan_switch_to_fiber` / `__tsan_destroy_fiber` (TSan) hooks across context switches.
- **Configurable Per-Routine Stack Sizes:** Added `gorun::StackSize` (`Small` 16 KB, `Medium` 64 KB, `Default` 128 KB, `Large` 256 KB, `XLarge` 512 KB, `Huge` 1 MB), `gorun::Options{.stack_size}`, and an 8-class multi-bucket `StackPool`.
- **Elastic Blocking Pool & Async File I/O (`include/gorun/blocking.hpp`):** Added `gorun::spawn_blocking` and non-blocking `gorun::fs` helpers (`read_file`, `write_file`, `append_file`, `file_size`, `exists`) to offload blocking OS/C-library calls without stalling M:N workers.
- **Bidirectional C++20 `co_await` Bridge (`include/gorun/coro.hpp`):** Made `JoinHandle<T>` and `Channel<T>` (`co_recv`, `co_send`) awaitable, and added `gorun::Task<T>` + `gorun::await_coro(...)` to await external C++20 coroutines from inside green routines.
- **Live Scheduler Observability:** Added `Runtime::stats()` returning `RuntimeStats` (workers, active/parked/running routines, global/local queue depths, work-stealing count, pending timers, and `StackPool` metrics).
- **Packaging & Reference Server:** Added vcpkg (`packaging/vcpkg/`) and Conan 2.0 (`packaging/conan/`) recipes, a zero-dependency native HTTP/1.1 server example (`examples/06_native_http_server.cpp`), v0.4.0 test suite (`tests/test_v04_features.cpp`), and macOS ARM64 + Linux TSan jobs in CI.

### Fixed
- **Deterministic Structured Concurrency Cleanup:** Destroyed task captures and flushed `RoutineLocal<T>` (`cur->clear_locals()`) strictly **before** signaling completion in `TaskGroup::spawn` (`wg_.done()`), `go_async` (`mark_ready()`), and `Runtime::block_on` (`notify_done()`), eliminating a shutdown race where `~Scope()` could return on another core before a child routine's `RoutineLocal<T>` destructors finished (`[TEST 24]`).
- **Scheduler Wakeups:** Improved external `global_queue_` worker notification and early-wake reset to prevent sleep stalls under bursty workloads.

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
