#pragma once
// =====================================================================================
//  gorun — single-header distribution (GENERATED FILE, DO NOT EDIT)
//  M:N Green Threads, Channels, select(), Context, Defer & Structured Concurrency
//  for C++20/23.
//
//  Source:  https://github.com/Takatochi/gorun
//  Author:  Artur Havor
//  License: Apache License 2.0
//
//  Regenerate with: python scripts/amalgamate.py
// =====================================================================================
// ---- begin gorun/gorun.hpp ----

/**
 * @file gorun.hpp
 * @brief Umbrella header for the `gorun` C++20/23 M:N Green Threads, Channels & Structured Concurrency Runtime.
 *
 * Include this single header to access the entire `gorun` API:
 * - Launching: `gorun::go(...)`, `gorun::go_in(...)`, `gorun::go_async(...)`
 * - Channels: `gorun::Channel<T>`, `gorun::select(...)`, `gorun::on_timeout(...)`, `gorun::on_default(...)`
 * - Go-Style Context: `gorun::Context`, `gorun::background()`, `gorun::with_cancel(...)`, `gorun::with_timeout(...)`, `gorun::with_deadline(...)`, `gorun::with_value(...)`
 * - Structured Concurrency: `gorun::Scope`, `gorun::TaskGroup`, `gorun::JoinHandle<T>`
 * - Green Synchronization: `gorun::Mutex`, `gorun::CondVar`, `gorun::WaitGroup`, `gorun::Semaphore`, `gorun::Once`, `gorun::RwMutex`
 * - Scope Cleanup: `gorun::defer(...)`, `GORUN_DEFER { ... };`
 * - Routine Control: `gorun::yield()`, `gorun::sleep_for(...)`, `gorun::this_routine::id()`, `gorun::RoutineLocal<T>`
 */

// ---- begin gorun/detail/spinlock.hpp ----

/**
 * @file spinlock.hpp
 * @brief Low-level core primitives for gorun: TTAS SpinLock and zero-allocation intrusive FIFO queue.
 *
 * These primitives are used internally by the M:N scheduler and synchronization objects
 * (`Channel<T>`, `Mutex`, `WaitGroup`, `CondVar`, `Semaphore`, `RwMutex`) to atomically
 * enqueue a routine's stack-allocated waiter node before performing a user-space `context_switch`.
 */

#include <atomic>
#include <cstdint>
#include <semaphore>
#include <utility>

#if defined(_MSC_VER) || defined(__MINGW32__) || defined(_WIN32)
#include <immintrin.h>
#elif defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#endif

namespace gorun::detail {

/**
 * @brief Emits a CPU spin-wait hint to reduce power consumption and memory bus contention.
 *
 * Executes the `PAUSE` instruction (`_mm_pause`) on x86/x86_64 and `YIELD` on ARM/AArch64.
 */
inline void cpu_relax() noexcept {
#if defined(_MSC_VER) || defined(__x86_64__) || defined(__i386__)
    _mm_pause();
#elif defined(__aarch64__) || defined(__arm__)
    __asm__ __volatile__("yield" ::: "memory");
#else
    std::atomic_signal_fence(std::memory_order_seq_cst);
#endif
}

/**
 * @class SpinLock
 * @brief Lightweight TTAS (Test-and-Test-and-Set) spinlock protecting internal wait queues.
 *
 * @note Inspired by Go's internal runtime `runtime.mutex` protecting `hchan.lock` and `semaRoot`.
 * @warning This spinlock is intended **exclusively** for ultra-short runtime critical sections
 * (a few pointer manipulations in `IntrusiveQueue`). User code inside green routines must use `gorun::Mutex`.
 */
class SpinLock {
    std::atomic<bool> locked_{false};

public:
    constexpr SpinLock() noexcept = default;
    SpinLock(const SpinLock&) = delete;
    SpinLock& operator=(const SpinLock&) = delete;

    /**
     * @brief Acquires the spinlock with `std::memory_order_acquire` semantics.
     *
     * Spins using read-only `std::memory_order_relaxed` loads while contended to avoid
     * cache-line invalidation traffic across CPU cores.
     */
    void lock() noexcept {
        for (;;) {
            if (!locked_.exchange(true, std::memory_order_acquire)) {
                return;
            }
            while (locked_.load(std::memory_order_relaxed)) {
                cpu_relax();
            }
        }
    }

    /**
     * @brief Attempts to acquire the spinlock without spinning.
     * @return `true` if the lock was acquired, `false` if already held by another thread.
     */
    [[nodiscard]] bool try_lock() noexcept {
        return !locked_.load(std::memory_order_relaxed) &&
               !locked_.exchange(true, std::memory_order_acquire);
    }

    /**
     * @brief Releases the spinlock with `std::memory_order_release` semantics.
     */
    void unlock() noexcept {
        locked_.store(false, std::memory_order_release);
    }
};

/**
 * @class IntrusiveQueue
 * @brief Doubly-linked intrusive FIFO queue with O(1) operations and zero heap allocations.
 *
 * @note Inspired by Go's `waitq` of stack-allocated/pooled `sudog` descriptors in `runtime/chan.go`.
 * Because `gorun` guarantees the **Stack Stability** invariant (routine stacks never relocate in memory),
 * waiter nodes (`Node`) are allocated directly on the sleeping routine's stack frame right before `park_locked()`.
 *
 * @tparam Node Intrusive node type providing `Node* prev` and `Node* next` members.
 */
template <typename Node>
class IntrusiveQueue {
    Node* head_{nullptr};
    Node* tail_{nullptr};

public:
    constexpr IntrusiveQueue() noexcept = default;

    IntrusiveQueue(const IntrusiveQueue&) = delete;
    IntrusiveQueue& operator=(const IntrusiveQueue&) = delete;

    /**
     * @brief Moves all queued nodes in O(1) time (used for batch wakeups in `notify_all()` and `Channel::close()`).
     */
    IntrusiveQueue(IntrusiveQueue&& other) noexcept
        : head_(std::exchange(other.head_, nullptr))
        , tail_(std::exchange(other.tail_, nullptr)) {}

    IntrusiveQueue& operator=(IntrusiveQueue&& other) noexcept {
        if (this != &other) {
            head_ = std::exchange(other.head_, nullptr);
            tail_ = std::exchange(other.tail_, nullptr);
        }
        return *this;
    }

    /**
     * @brief Checks whether the queue contains no elements.
     * @return `true` if the queue is empty.
     */
    [[nodiscard]] bool empty() const noexcept {
        return head_ == nullptr;
    }

    /**
     * @brief Appends a waiter node to the tail of the queue in O(1) time.
     * @param node Pointer to a stack-allocated waiter node.
     */
    void push_back(Node* node) noexcept {
        node->next = nullptr;
        node->prev = tail_;
        if (tail_) {
            tail_->next = node;
        } else {
            head_ = node;
        }
        tail_ = node;
    }

    /**
     * @brief Removes and returns the oldest node from the head of the queue (FIFO) in O(1) time.
     * @return Pointer to the dequeued node, or `nullptr` if the queue is empty.
     */
    Node* pop_front() noexcept {
        if (!head_) {
            return nullptr;
        }
        Node* node = head_;
        head_ = head_->next;
        if (head_) {
            head_->prev = nullptr;
        } else {
            tail_ = nullptr;
        }
        node->next = nullptr;
        node->prev = nullptr;
        return node;
    }

    /**
     * @brief Unlinks an arbitrary node from the queue in O(1) time.
     * @param node Pointer to the node to remove.
     */
    void remove(Node* node) noexcept {
        if (node->prev) {
            node->prev->next = node->next;
        } else if (head_ == node) {
            head_ = node->next;
        }

        if (node->next) {
            node->next->prev = node->prev;
        } else if (tail_ == node) {
            tail_ = node->prev;
        }

        node->prev = nullptr;
        node->next = nullptr;
    }
};

} // namespace gorun::detail
// ---- end gorun/detail/spinlock.hpp ----
// ---- begin gorun/detail/stack_pool.hpp ----

/**
 * @file stack_pool.hpp
 * @brief Virtual memory stack allocator and recycling pool (`StackPool`) with hardware Guard Pages.
 */

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <mutex>
#include <new>
#include <vector>

#if defined(_WIN32)
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <sys/mman.h>
#include <unistd.h>
#endif

namespace gorun::detail {

/**
 * @struct StackAllocation
 * @brief Descriptor for an OS-allocated green routine stack with an inaccessible hardware Guard Page at the bottom.
 *
 * Guarantees the **Stack Stability** invariant: unlike Go's growable/relocatable stacks (which invalidate
 * interior pointers unless escape-analyzed to the heap), `gorun` allocates fixed virtual-memory stacks
 * that never move during the routine's lifetime. Taking pointers/references to stack variables across
 * suspension points (`yield`, `Channel::recv`, `Mutex::lock`) is 100% safe.
 */
struct StackAllocation {
    void* base_allocation{nullptr}; ///< Base address returned by `VirtualAlloc` / `mmap` (starts with the Guard Page)
    std::size_t total_bytes{0};     ///< Total virtual memory region size in bytes (usable stack + guard page)
    void* stack_bottom{nullptr};    ///< Lowest usable stack address (immediately above the guard page)
    void* stack_top{nullptr};       ///< Highest usable stack address (initial `RSP`; stack grows downward)
    std::size_t usable_bytes{0};    ///< Usable stack capacity in bytes
};

/**
 * @class StackPool
 * @brief Thread-safe recycling pool of routine stacks (`StackAllocation`).
 *
 * Avoids expensive `VirtualAlloc`/`VirtualFree` or `mmap`/`munmap` system calls on every `gorun::go(...)` spawn
 * by caching completed routine stacks in LIFO order.
 */
class StackPool {
    static constexpr std::size_t kMinStackBytes = 16 * 1024; ///< Minimum safe usable stack size (16 KB)

    std::size_t default_usable_size_;
    std::size_t max_pooled_;
    mutable std::mutex mutex_;
    std::vector<StackAllocation> pool_;

    /**
     * @brief Queries the operating system virtual memory page size (typically 4096 bytes).
     */
    static std::size_t page_size() noexcept {
#if defined(_WIN32)
        SYSTEM_INFO si;
        GetSystemInfo(&si);
        return si.dwPageSize > 0 ? static_cast<std::size_t>(si.dwPageSize) : 4096;
#else
        long sz = ::sysconf(_SC_PAGESIZE);
        return sz > 0 ? static_cast<std::size_t>(sz) : 4096;
#endif
    }

    static std::size_t align_up(std::size_t size, std::size_t alignment) noexcept {
        return (size + alignment - 1) & ~(alignment - 1);
    }

    /**
     * @brief Allocates a fresh stack from OS virtual memory and marks the lowest page as `PAGE_NOACCESS` / `PROT_NONE`.
     * @param requested_usable Requested usable stack size in bytes (clamped to >= 16 KB, overflow-checked).
     * @throws std::bad_alloc If OS virtual memory allocation fails or size overflows.
     */
    static StackAllocation os_allocate(std::size_t requested_usable) {
        const std::size_t pg = page_size();
        const std::size_t clamped = std::max(requested_usable, kMinStackBytes);
        if (clamped > std::numeric_limits<std::size_t>::max() - 2 * pg) {
            throw std::bad_alloc();
        }
        const std::size_t usable = align_up(clamped, pg);
        const std::size_t total = usable + pg; // +1 page at the bottom for hardware Guard Page

#if defined(_WIN32)
        void* raw = ::VirtualAlloc(nullptr, total, MEM_RESERVE | MEM_COMMIT, PAGE_READWRITE);
        if (!raw) {
            throw std::bad_alloc();
        }
        DWORD old_protect = 0;
        if (!::VirtualProtect(raw, pg, PAGE_NOACCESS, &old_protect)) {
            ::VirtualFree(raw, 0, MEM_RELEASE);
            throw std::bad_alloc();
        }
#else
        void* raw = ::mmap(nullptr, total, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (raw == MAP_FAILED) {
            throw std::bad_alloc();
        }
        if (::mprotect(raw, pg, PROT_NONE) != 0) {
            ::munmap(raw, total);
            throw std::bad_alloc();
        }
#endif
        auto* byte_ptr = static_cast<std::uint8_t*>(raw);
        return StackAllocation{
            .base_allocation = raw,
            .total_bytes = total,
            .stack_bottom = byte_ptr + pg,
            .stack_top = byte_ptr + total,
            .usable_bytes = usable
        };
    }

    static void os_deallocate(const StackAllocation& alloc) noexcept {
        if (!alloc.base_allocation) return;
#if defined(_WIN32)
        ::VirtualFree(alloc.base_allocation, 0, MEM_RELEASE);
#else
        ::munmap(alloc.base_allocation, alloc.total_bytes);
#endif
    }

public:
    /**
     * @brief Constructs a `StackPool`.
     * @param default_usable_size Default usable stack size per routine in bytes (default: 128 KB, minimum 16 KB).
     * @param max_pooled Maximum number of idle stacks retained in the pool before returning memory to the OS.
     */
    explicit StackPool(std::size_t default_usable_size = 128 * 1024, std::size_t max_pooled = 1024)
        : default_usable_size_(std::max(default_usable_size, kMinStackBytes))
        , max_pooled_(max_pooled) {}

    ~StackPool() {
        std::lock_guard lock(mutex_);
        for (const auto& s : pool_) {
            os_deallocate(s);
        }
        pool_.clear();
    }

    /**
     * @brief Acquires a warm stack from the pool (LIFO) or allocates a new one from the OS.
     * @return `StackAllocation` descriptor.
     */
    StackAllocation acquire() {
        {
            std::lock_guard lock(mutex_);
            if (!pool_.empty()) {
                StackAllocation s = pool_.back();
                pool_.pop_back();
                return s;
            }
        }
        return os_allocate(default_usable_size_);
    }

    /**
     * @brief Returns a finished routine's stack to the recycling pool.
     * @param s Stack allocation descriptor to recycle.
     */
    void release(StackAllocation s) noexcept {
        if (!s.base_allocation) return;
        {
            std::lock_guard lock(mutex_);
            if (pool_.size() < max_pooled_) {
                pool_.push_back(s);
                return;
            }
        }
        os_deallocate(s);
    }

    /**
     * @brief Returns the current number of cached idle stacks in the pool.
     */
    [[nodiscard]] std::size_t pooled_count() const noexcept {
        std::lock_guard lock(mutex_);
        return pool_.size();
    }
};

} // namespace gorun::detail
// ---- end gorun/detail/stack_pool.hpp ----
// ---- begin gorun/detail/context.hpp ----

/**
 * @file context.hpp
 * @brief Ultra-fast user-space CPU execution context (`ExecutionContext`) and assembly stack switching.
 *
 * Supports cross-thread migration of green routines across M:N OS worker threads (Work-Stealing)
 * on both Windows x64 ABI (saving GPRs, XMM6–XMM15, MXCSR, x87 FCW, and `NT_TIB` stack limits)
 * and System V AMD64 ABI (Linux/macOS, saving GPRs, MXCSR, and x87 FCW).
 */

#include <atomic>
#include <cstddef>
#include <cstdint>


namespace gorun::detail {

#if defined(_MSC_VER) && !defined(__clang__)
#error "gorun requires a GNU/Clang inline-assembly compatible compiler (GCC, Clang, or MinGW-w64) on x86_64."
#elif defined(__x86_64__) || defined(_M_X64)

extern "C" void gorun_context_switch_asm(void** from_rsp, void** to_rsp) noexcept;
extern "C" void gorun_routine_entry_asm() noexcept;

// Default IEEE-754 MXCSR (0x1F80: all SSE exceptions masked, round-to-nearest)
// combined with default x87 FPU control word (0x037F: all x87 exceptions masked, 64-bit precision)
inline constexpr std::uint64_t kDefaultFpuMxcsrState =
    (static_cast<std::uint64_t>(0x037Fu) << 32) | 0x1F80u;

#if defined(_WIN32)
__asm__(
    ".text\n"
    ".globl gorun_context_switch_asm\n"
    "gorun_context_switch_asm:\n"
    "  pushq %rbp\n"
    "  pushq %rbx\n"
    "  pushq %rdi\n"
    "  pushq %rsi\n"
    "  pushq %r12\n"
    "  pushq %r13\n"
    "  pushq %r14\n"
    "  pushq %r15\n"
    // Save Windows NT_TIB stack bounds (StackBase=gs:0x08, StackLimit=gs:0x10, DeallocationStack=gs:0x1478)
    "  movq %gs:0x08, %rax\n"
    "  pushq %rax\n"
    "  movq %gs:0x10, %rax\n"
    "  pushq %rax\n"
    "  movq %gs:0x1478, %rax\n"
    "  pushq %rax\n"
    // Save callee-saved XMM6..XMM15 (160 bytes) + MXCSR (4 bytes) + x87 FCW (2 bytes) = 168 bytes
    "  subq $168, %rsp\n"
    "  movups %xmm6,   0(%rsp)\n"
    "  movups %xmm7,  16(%rsp)\n"
    "  movups %xmm8,  32(%rsp)\n"
    "  movups %xmm9,  48(%rsp)\n"
    "  movups %xmm10, 64(%rsp)\n"
    "  movups %xmm11, 80(%rsp)\n"
    "  movups %xmm12, 96(%rsp)\n"
    "  movups %xmm13, 112(%rsp)\n"
    "  movups %xmm14, 128(%rsp)\n"
    "  movups %xmm15, 144(%rsp)\n"
    "  stmxcsr 160(%rsp)\n"
    "  fnstcw  164(%rsp)\n"
    // Switch stack pointer RSP: rcx = from_rsp, rdx = to_rsp
    "  movq %rsp, (%rcx)\n"
    "  movq (%rdx), %rsp\n"
    // Restore XMM6..XMM15 + MXCSR + x87 FCW
    "  movups   0(%rsp), %xmm6\n"
    "  movups  16(%rsp), %xmm7\n"
    "  movups  32(%rsp), %xmm8\n"
    "  movups  48(%rsp), %xmm9\n"
    "  movups  64(%rsp), %xmm10\n"
    "  movups  80(%rsp), %xmm11\n"
    "  movups  96(%rsp), %xmm12\n"
    "  movups 112(%rsp), %xmm13\n"
    "  movups 128(%rsp), %xmm14\n"
    "  movups 144(%rsp), %xmm15\n"
    "  ldmxcsr 160(%rsp)\n"
    "  fldcw   164(%rsp)\n"
    "  addq $168, %rsp\n"
    // Restore Windows NT_TIB stack bounds on the target OS thread
    "  popq %rax\n"
    "  movq %rax, %gs:0x1478\n"
    "  popq %rax\n"
    "  movq %rax, %gs:0x10\n"
    "  popq %rax\n"
    "  movq %rax, %gs:0x08\n"
    // Restore callee-saved GPRs
    "  popq %r15\n"
    "  popq %r14\n"
    "  popq %r13\n"
    "  popq %r12\n"
    "  popq %rsi\n"
    "  popq %rdi\n"
    "  popq %rbx\n"
    "  popq %rbp\n"
    "  retq\n"
    ".globl gorun_routine_entry_asm\n"
    "gorun_routine_entry_asm:\n"
    "  subq $40, %rsp\n" // 32-byte Win64 shadow space + 8-byte 16B alignment
    "  movq %r12, %rcx\n"
    "  callq *%r13\n"
    "  ud2\n"
);
#else
__asm__(
    ".text\n"
    ".globl gorun_context_switch_asm\n"
    ".type gorun_context_switch_asm, @function\n"
    "gorun_context_switch_asm:\n"
    "  .cfi_startproc\n"
    "  pushq %rbp\n"
    "  pushq %rbx\n"
    "  pushq %r12\n"
    "  pushq %r13\n"
    "  pushq %r14\n"
    "  pushq %r15\n"
    "  subq $8, %rsp\n"
    "  stmxcsr 0(%rsp)\n"
    "  fnstcw  4(%rsp)\n"
    "  movq %rsp, (%rdi)\n"
    "  movq (%rsi), %rsp\n"
    "  ldmxcsr 0(%rsp)\n"
    "  fldcw   4(%rsp)\n"
    "  addq $8, %rsp\n"
    "  popq %r15\n"
    "  popq %r14\n"
    "  popq %r13\n"
    "  popq %r12\n"
    "  popq %rbx\n"
    "  popq %rbp\n"
    "  retq\n"
    "  .cfi_endproc\n"
    ".globl gorun_routine_entry_asm\n"
    ".type gorun_routine_entry_asm, @function\n"
    "gorun_routine_entry_asm:\n"
    "  .cfi_startproc\n"
    "  subq $8, %rsp\n" // Align RSP so inside `callq *%r13`, RSP % 16 == 8 (System V AMD64 ABI)
    "  movq %r12, %rdi\n"
    "  callq *%r13\n"
    "  ud2\n"
    "  .cfi_endproc\n"
);
#endif

/**
 * @struct ExecutionContext
 * @brief Hardware execution context for a green routine or worker scheduler loop (analogous to Go's `gobuf`).
 *
 * Stores the stack pointer `rsp` pointing to the saved register frame on the routine's isolated stack.
 */
struct ExecutionContext {
    void* rsp{nullptr}; ///< Saved stack pointer (`%rsp`)

    void init_scheduler_for_current_thread() noexcept {}
    void cleanup_scheduler_for_current_thread() noexcept {}
    void destroy_routine_context() noexcept {}

    /**
     * @brief Initializes a fresh stack frame on `stack` so that the first `context_switch` jumps into `trampoline(arg)`.
     * @param stack Allocated virtual memory stack region from `StackPool`.
     * @param trampoline Entry-point function pointer (`Runtime::routine_trampoline`).
     * @param arg Pointer to the owning `Routine` instance.
     */
    void make_routine_stack_context(const StackAllocation& stack, void (*trampoline)(void*), void* arg) noexcept {
        auto top = reinterpret_cast<std::uintptr_t>(stack.stack_top);
        top &= ~static_cast<std::uintptr_t>(15);

        auto* sp = reinterpret_cast<std::uint64_t*>(top);
        *(--sp) = 0; // alignment padding
        *(--sp) = reinterpret_cast<std::uint64_t>(&gorun_routine_entry_asm); // Return RIP for retq
        *(--sp) = 0; // rbp
        *(--sp) = 0; // rbx
#if defined(_WIN32)
        *(--sp) = 0; // rdi
        *(--sp) = 0; // rsi
#endif
        *(--sp) = reinterpret_cast<std::uint64_t>(arg);        // r12 -> 1st argument (rcx / rdi)
        *(--sp) = reinterpret_cast<std::uint64_t>(trampoline); // r13 -> target trampoline function
        *(--sp) = 0; // r14
        *(--sp) = 0; // r15
#if defined(_WIN32)
        // Windows NT_TIB stack limits: StackBase, StackLimit, DeallocationStack
        *(--sp) = reinterpret_cast<std::uint64_t>(stack.stack_top);       // gs:0x08 StackBase
        *(--sp) = reinterpret_cast<std::uint64_t>(stack.stack_bottom);    // gs:0x10 StackLimit
        *(--sp) = reinterpret_cast<std::uint64_t>(stack.base_allocation); // gs:0x1478 DeallocationStack
        // MXCSR + x87 FCW slot at 160(%rsp)
        *(--sp) = kDefaultFpuMxcsrState;
        // 160 bytes (20 * uint64_t) for XMM6..XMM15 at 0..144(%rsp)
        for (int i = 0; i < 20; ++i) {
            *(--sp) = 0;
        }
#else
        // MXCSR + x87 FCW slot at 0(%rsp)
        *(--sp) = kDefaultFpuMxcsrState;
#endif
        rsp = sp;
    }
};

/**
 * @brief Performs an ultra-fast (~10–15 ns) user-space context switch between `from` and `to`.
 *
 * Marked `noinline` with compiler signal fences so optimizing compilers (`-O2`/`-O3`) never cache
 * `thread_local` pointers (`current_worker()`, `current_routine()`) in registers across a routine migration
 * between different OS worker threads.
 *
 * @param from Context where the current CPU registers and `%rsp` will be saved.
 * @param to Context whose CPU registers and `%rsp` will be restored.
 */
#if defined(__GNUC__) || defined(__clang__)
__attribute__((noinline))
#elif defined(_MSC_VER)
__declspec(noinline)
#endif
inline void context_switch(ExecutionContext* from, ExecutionContext* to) noexcept {
    std::atomic_signal_fence(std::memory_order_seq_cst);
    gorun_context_switch_asm(&from->rsp, &to->rsp);
    std::atomic_signal_fence(std::memory_order_seq_cst);
}

#else
#error "gorun: unsupported CPU architecture (x86_64 is currently required)."
#endif

} // namespace gorun::detail
// ---- end gorun/detail/context.hpp ----
// ---- begin gorun/detail/work_stealing_deque.hpp ----

/**
 * @file work_stealing_deque.hpp
 * @brief Per-worker double-ended run queue supporting LIFO local execution and FIFO work-stealing.
 */

#include <atomic>
#include <cstddef>
#include <mutex>
#include <optional>
#include <vector>


namespace gorun::detail {

/**
 * @class WorkStealingDeque
 * @brief Double-ended circular run queue for an M:N worker thread.
 *
 * @note Inspired by Go's per-processor (`P`) run queue (`runq` / `runnext` in `runtime/proc.go`).
 *
 * Combines cache locality with load balancing:
 * - **Owner Worker** pushes and pops from the tail (`push_back` / `pop_back`) in **LIFO** order so that
 *   newly spawned or freshly unparked routines execute immediately while their working set is hot in L1/L2 cache.
 * - **Thief Workers** steal half of the victim queue from the head (`steal_half_into`) in **FIFO** order,
 *   acquiring the oldest (and typically largest/most independent) tasks in a single batched lock acquisition.
 *
 * @tparam T Element type stored in the deque (typically `Routine*`).
 */
template <typename T>
class WorkStealingDeque {
    mutable SpinLock lock_;
    T run_next_{nullptr}; ///< 1-element hot LIFO slot (Go's `p.runnext`), protected by lock_ so thieves can also steal it!
    std::vector<T> buffer_;
    std::size_t head_{0};
    std::size_t tail_{0};
    std::size_t count_{0};

    void grow_if_needed() {
        if (count_ < buffer_.size()) {
            return;
        }
        const std::size_t old_cap = buffer_.size();
        const std::size_t new_cap = old_cap == 0 ? 256 : old_cap * 2;
        std::vector<T> next_buf(new_cap);
        for (std::size_t i = 0; i < count_; ++i) {
            next_buf[i] = buffer_[(head_ + i) % old_cap];
        }
        buffer_ = std::move(next_buf);
        head_ = 0;
        tail_ = count_;
    }

public:
    /**
     * @brief Constructs a `WorkStealingDeque` with the given initial capacity.
     * @param initial_capacity Initial circular buffer size (default: 256 elements).
     */
    explicit WorkStealingDeque(std::size_t initial_capacity = 256)
        : buffer_(initial_capacity) {}

    /**
     * @brief Places `item` into the hot `run_next_` slot (like Go's `runqput(..., next=true)`).
     * If `run_next_` was already occupied, the displaced routine is pushed to the tail of the circular buffer.
     * @return `true` if a previous routine was displaced (meaning >=2 tasks are queued locally), `false` otherwise.
     */
    bool push_run_next(T item) {
        std::lock_guard guard(lock_);
        if (!run_next_) {
            run_next_ = item;
            return count_ > 0;
        }
        T displaced = std::exchange(run_next_, item);
        grow_if_needed();
        buffer_[tail_] = displaced;
        tail_ = (tail_ + 1) % buffer_.size();
        ++count_;
        return true;
    }

    /**
     * @brief Pushes a routine to the tail of the deque.
     * @param item Routine pointer to enqueue.
     */
    void push_back(T item) {
        std::lock_guard guard(lock_);
        grow_if_needed();
        buffer_[tail_] = item;
        tail_ = (tail_ + 1) % buffer_.size();
        ++count_;
    }

    /**
     * @brief Pushes a yielded routine to the head of the deque (FIFO fair placement) so other queued
     *        routines run before it on LIFO `pop_back()`, preventing self-starvation on `yield()`.
     * @param item Routine pointer to enqueue.
     */
    void push_front(T item) {
        std::lock_guard guard(lock_);
        grow_if_needed();
        head_ = (head_ + buffer_.size() - 1) % buffer_.size();
        buffer_[head_] = item;
        ++count_;
    }

    /**
     * @brief Pops the most recently pushed routine: checks `run_next_` first, then the tail of the circular buffer (LIFO).
     * @return Routine pointer, or `nullptr` if empty.
     */
    T pop_back() noexcept {
        std::lock_guard guard(lock_);
        if (run_next_) {
            return std::exchange(run_next_, nullptr);
        }
        if (count_ == 0) {
            return nullptr;
        }
        tail_ = (tail_ + buffer_.size() - 1) % buffer_.size();
        T item = buffer_[tail_];
        --count_;
        return item;
    }

    /**
     * @brief Pops the oldest routine from the head (FIFO — used by the Global Run Queue).
     * @return Routine pointer, or `nullptr` if empty.
     */
    T pop_front() noexcept {
        std::lock_guard guard(lock_);
        if (count_ > 0) {
            T item = buffer_[head_];
            head_ = (head_ + 1) % buffer_.size();
            --count_;
            return item;
        }
        if (run_next_) {
            return std::exchange(run_next_, nullptr);
        }
        return nullptr;
    }

    /**
     * @brief Steals half of the queued tasks from the head of this deque (FIFO), including `run_next_` if the circular
     *        buffer is empty, and transfers them into `dst_deque`.
     *
     * @param dst_deque Local deque of the stealing worker thread.
     * @return The first stolen routine (to be executed immediately by the thief), or `nullptr` if this deque was empty.
     */
    T steal_half_into(WorkStealingDeque<T>& dst_deque) {
        std::vector<T> stolen;
        {
            std::lock_guard guard(lock_);
            if (count_ == 0) {
                if (run_next_) {
                    return std::exchange(run_next_, nullptr);
                }
                return nullptr;
            }
            std::size_t to_steal = (count_ + 1) / 2;
            stolen.reserve(to_steal);
            for (std::size_t i = 0; i < to_steal; ++i) {
                stolen.push_back(buffer_[head_]);
                head_ = (head_ + 1) % buffer_.size();
                --count_;
            }
        }

        T first = stolen.front();
        if (stolen.size() > 1) {
            for (std::size_t i = 1; i < stolen.size(); ++i) {
                dst_deque.push_back(stolen[i]);
            }
        }
        return first;
    }

    /**
     * @brief Returns the current number of elements in the deque (including `run_next_`).
     */
    [[nodiscard]] std::size_t size() const noexcept {
        std::lock_guard guard(lock_);
        return count_ + (run_next_ != nullptr ? 1 : 0);
    }

    /**
     * @brief Checks whether both `run_next_` and the circular buffer are empty.
     */
    [[nodiscard]] bool empty() const noexcept {
        std::lock_guard guard(lock_);
        return count_ == 0 && run_next_ == nullptr;
    }
};

} // namespace gorun::detail
// ---- end gorun/detail/work_stealing_deque.hpp ----
// ---- begin gorun/defer.hpp ----

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
// ---- end gorun/defer.hpp ----
// ---- begin gorun/runtime.hpp ----

/**
 * @file runtime.hpp
 * @brief Core M:N work-stealing scheduler (`Runtime`, `Worker`, `Routine`), cancellable timer thread,
 *        lock-free `RoutineLocal<T>`, and `this_routine` namespace.
 */

#include <atomic>
#include <chrono>
#include <concepts>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <random>
#include <semaphore>
#include <stop_token>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#if defined(__SANITIZE_ADDRESS__)
#define GORUN_HAS_ASAN 1
#elif defined(__has_feature)
#if __has_feature(address_sanitizer)
#define GORUN_HAS_ASAN 1
#endif
#endif

#if defined(GORUN_HAS_ASAN)
extern "C" {
void __sanitizer_start_switch_fiber(void** fake_stack_save, const void* bottom, std::size_t size);
void __sanitizer_finish_switch_fiber(void* fake_stack_save, const void** bottom_old, std::size_t* size_old);
}
#endif






namespace gorun {

class Runtime;
class TaskGroup;

/**
 * @struct Config
 * @brief Configuration options for a `gorun::Runtime` instance.
 */
struct Config {
    /**
     * @brief Number of OS worker threads (`std::jthread`) multiplexing green routines.
     * Defaults to `std::thread::hardware_concurrency()` (analogous to `GOMAXPROCS` in Go).
     */
    std::size_t worker_count = std::thread::hardware_concurrency() > 0
                                   ? std::thread::hardware_concurrency()
                                   : 4;

    /**
     * @brief Usable virtual-memory stack size per green routine in bytes (default: 128 KB, minimum 16 KB).
     */
    std::size_t default_stack_size = 128 * 1024;

    /**
     * @brief Maximum number of idle routine stacks cached in `StackPool` for zero-syscall reuse.
     */
    std::size_t max_pooled_stacks = 1024;
};

namespace detail {

class Worker;

/**
 * @class UniqueTask
 * @brief C++20 move-only type-erased callable wrapper (`void()`) with 32-byte Small Buffer Optimization (SBO),
 *        enabling zero-allocation capture of typical lambdas and move-only lambdas (e.g., `std::unique_ptr<T>`)
 *        in `gorun::go(...)`, `TaskGroup::spawn(...)`, and `go_async(...)`.
 */
class UniqueTask {
    static constexpr std::size_t kInlineSize = 32;

    struct VTable {
        void (*invoke)(void* storage);
        void (*move_to)(void* dst, void* src) noexcept;
        void (*destroy)(void* storage) noexcept;
    };

    template <typename F>
    static constexpr bool kFitsInline =
        sizeof(F) <= kInlineSize &&
        alignof(F) <= alignof(std::max_align_t) &&
        std::is_nothrow_move_constructible_v<F>;

    template <typename F>
    static const VTable* vtable_for() noexcept {
        if constexpr (kFitsInline<F>) {
            static constexpr VTable vt{
                [](void* s) { (*static_cast<F*>(s))(); },
                [](void* dst, void* src) noexcept {
                    ::new (dst) F(std::move(*static_cast<F*>(src)));
                    static_cast<F*>(src)->~F();
                },
                [](void* s) noexcept { static_cast<F*>(s)->~F(); }
            };
            return &vt;
        } else {
            static constexpr VTable vt{
                [](void* s) {
                    F* p = nullptr;
                    std::memcpy(&p, s, sizeof(p));
                    (*p)();
                },
                [](void* dst, void* src) noexcept {
                    std::memcpy(dst, src, sizeof(F*));
                },
                [](void* s) noexcept {
                    F* p = nullptr;
                    std::memcpy(&p, s, sizeof(p));
                    delete p;
                }
            };
            return &vt;
        }
    }

    alignas(std::max_align_t) std::byte storage_[kInlineSize];
    const VTable* vtable_{nullptr};

public:
    UniqueTask() noexcept = default;
    UniqueTask(std::nullptr_t) noexcept {}

    template <typename F>
        requires (!std::same_as<std::decay_t<F>, UniqueTask> && std::invocable<std::decay_t<F>&>)
    UniqueTask(F&& f) {
        using Decayed = std::decay_t<F>;
        if constexpr (kFitsInline<Decayed>) {
            ::new (static_cast<void*>(storage_)) Decayed(std::forward<F>(f));
        } else {
            Decayed* p = new Decayed(std::forward<F>(f));
            std::memcpy(storage_, &p, sizeof(p));
        }
        vtable_ = vtable_for<Decayed>();
    }

    UniqueTask(UniqueTask&& other) noexcept {
        if (other.vtable_) {
            other.vtable_->move_to(storage_, other.storage_);
            vtable_ = std::exchange(other.vtable_, nullptr);
        }
    }

    UniqueTask& operator=(UniqueTask&& other) noexcept {
        if (this != &other) {
            reset();
            if (other.vtable_) {
                other.vtable_->move_to(storage_, other.storage_);
                vtable_ = std::exchange(other.vtable_, nullptr);
            }
        }
        return *this;
    }

    UniqueTask& operator=(std::nullptr_t) noexcept {
        reset();
        return *this;
    }

    ~UniqueTask() noexcept { reset(); }

    UniqueTask(const UniqueTask&) = delete;
    UniqueTask& operator=(const UniqueTask&) = delete;

    void reset() noexcept {
        if (vtable_) {
            vtable_->destroy(storage_);
            vtable_ = nullptr;
        }
    }

    explicit operator bool() const noexcept { return vtable_ != nullptr; }

    void operator()() {
        if (vtable_) {
            vtable_->invoke(storage_);
        }
    }
};

/**
 * @enum RoutineState
 * @brief Lifecycle states of a green routine (analogous to `_Grunnable`, `_Grunning`, `_Gwaiting`, `_Gdead` in Go).
 */
enum class RoutineState : std::uint8_t {
    Ready,    ///< Routine is runnable and queued in a local or global run queue
    Running,  ///< Routine is currently executing on an OS `Worker` thread
    Parked,   ///< Routine is suspended on a synchronization primitive or timer (consumes zero OS threads)
    Finished  ///< Routine body has returned; stack and descriptor are ready for recycling
};

/**
 * @struct Routine
 * @brief Internal representation of a lightweight green thread (analogous to `runtime.g` in Go).
 */
struct Routine {
    struct LocalSlot {
        void* ptr{nullptr};
        void (*deleter)(void*){nullptr};

        LocalSlot() = default;
        LocalSlot(void* p, void (*d)(void*)) noexcept : ptr(p), deleter(d) {}
        LocalSlot(const LocalSlot&) = delete;
        LocalSlot& operator=(const LocalSlot&) = delete;
        LocalSlot(LocalSlot&& other) noexcept
            : ptr(std::exchange(other.ptr, nullptr))
            , deleter(std::exchange(other.deleter, nullptr)) {}
        LocalSlot& operator=(LocalSlot&& other) noexcept {
            if (this != &other) {
                reset();
                ptr = std::exchange(other.ptr, nullptr);
                deleter = std::exchange(other.deleter, nullptr);
            }
            return *this;
        }
        ~LocalSlot() noexcept { reset(); }

        void reset() noexcept {
            if (ptr && deleter) {
                deleter(ptr);
            }
            ptr = nullptr;
            deleter = nullptr;
        }
    };

    std::uint64_t id{0};                        ///< Unique monotonically increasing routine ID
    RoutineState state{RoutineState::Ready};    ///< Current lifecycle state
    ExecutionContext ctx{};                     ///< Saved CPU registers and stack pointer (`%rsp`)
    StackAllocation stack{};                    ///< Virtual-memory stack region with hardware Guard Page
    UniqueTask task{};                          ///< Move-only user callable body
    std::stop_source stop_source{};             ///< Cooperative cancellation source for this routine
    Runtime* owner_runtime{nullptr};            ///< Owning `Runtime` instance
    TaskGroup* implicit_scope{nullptr};         ///< Active structured concurrency `TaskGroup` / `Scope`

    // Per-routine storage for RoutineLocal<T> — automatically destroyed when this routine finishes (zero leaks!)
    std::unordered_map<std::uint64_t, LocalSlot> routine_locals{};

    Routine* prev{nullptr};
    Routine* next{nullptr};
#if defined(GORUN_HAS_ASAN)
    void* asan_fake_stack{nullptr};
#endif

    void clear_locals() noexcept {
        routine_locals.clear();
    }
};

inline Worker*& tls_current_worker() noexcept {
    thread_local Worker* w = nullptr;
    return w;
}

inline Routine*& tls_current_routine() noexcept {
    thread_local Routine* r = nullptr;
    return r;
}

inline TaskGroup*& tls_implicit_scope() noexcept {
    thread_local TaskGroup* s = nullptr;
    return s;
}

/**
 * @brief Returns the `Worker` bound to the current OS thread, or `nullptr` if called from an external thread (e.g. `main()`).
 */
#if defined(__GNUC__) || defined(__clang__)
__attribute__((noinline))
#elif defined(_MSC_VER)
__declspec(noinline)
#endif
inline Worker* current_worker() noexcept {
    std::atomic_signal_fence(std::memory_order_seq_cst);
    return tls_current_worker();
}

/**
 * @brief Returns the green `Routine` currently running on this OS worker thread, or `nullptr`.
 */
#if defined(__GNUC__) || defined(__clang__)
__attribute__((noinline))
#elif defined(_MSC_VER)
__declspec(noinline)
#endif
inline Routine* current_routine() noexcept {
    std::atomic_signal_fence(std::memory_order_seq_cst);
    return tls_current_routine();
}

/**
 * @brief Returns the active structured concurrency `TaskGroup` (or `Scope`) for the current routine or OS thread.
 */
inline TaskGroup* current_implicit_scope() noexcept {
    if (Routine* r = current_routine()) {
        return r->implicit_scope;
    }
    return tls_implicit_scope();
}

/**
 * @brief Sets the active structured concurrency `TaskGroup` for the current routine or OS thread, returning the previous one.
 */
inline TaskGroup* set_implicit_scope(TaskGroup* s) noexcept {
    if (Routine* r = current_routine()) {
        return std::exchange(r->implicit_scope, s);
    }
    return std::exchange(tls_implicit_scope(), s);
}

/**
 * @struct UniversalWaiter
 * @brief Hybrid suspension node allowing `gorun` primitives (`Channel`, `Mutex`, `WaitGroup`) to be called
 *        seamlessly from both green routines and external OS threads (such as `int main()`).
 *
 * - **Inside a Green Routine (`current_worker() != nullptr`):** Suspends the routine via `Worker::park_locked()`
 *   using a user-space `context_switch` (**Zero OS-thread blocking**).
 * - **From an External OS Thread (`current_worker() == nullptr`):** Safely parks the calling OS thread on a
 *   stack-allocated `std::binary_semaphore`.
 */
struct UniversalWaiter {
    Routine* routine{nullptr};
    std::binary_semaphore* os_sem{nullptr};

    UniversalWaiter* prev{nullptr};
    UniversalWaiter* next{nullptr};

    static UniversalWaiter make_for_current(std::binary_semaphore& fallback_sem) noexcept {
        Routine* r = current_routine();
        return UniversalWaiter{
            .routine = r,
            .os_sem = (r == nullptr) ? &fallback_sem : nullptr
        };
    }

    /**
     * @brief Atomically parks the current green routine (or external OS thread) and releases `primitive_lock`.
     */
    void park_current(SpinLock& primitive_lock) noexcept;

    /**
     * @brief Unparks the waiting routine (re-schedules it onto a worker run queue) or wakes the external OS thread.
     */
    void unpark() noexcept;
};

} // namespace detail

/**
 * @class Runtime
 * @brief M:N work-stealing green thread runtime.
 *
 * Manages the pool of OS `Worker` threads, the `Global Run Queue`, the `StackPool`, and the background
 * cancellable `Timer Thread`. Can be instantiated explicitly for isolated pools (`gorun::Runtime rt(cfg);`)
 * or accessed implicitly via the lazy global singleton `gorun::default_runtime()` whenever `gorun::go(...)` is called.
 */
class Runtime {
private:
    friend class detail::Worker;
    friend struct detail::UniversalWaiter;

    Config config_;
    detail::StackPool stack_pool_;
    std::atomic<std::uint64_t> next_routine_id_{1};

    detail::WorkStealingDeque<detail::Routine*> global_queue_;

    std::vector<std::unique_ptr<detail::Worker>> workers_;
    std::vector<std::jthread> worker_threads_;

    std::mutex sleep_mutex_;
    std::condition_variable sleep_cv_;
    std::atomic<std::size_t> sleeping_workers_{0};
    std::atomic<std::uint32_t> waking_workers_{0};
    std::atomic<std::size_t> active_routines_{0};
    std::atomic<bool> shutdown_{false};

    // Cancellable timer queue (O(log N) insert, O(1) earliest peek, O(log N) cancel with zero timer leaks!)
    struct TimerEntry {
        std::uint64_t id{0};
        detail::Routine* routine{nullptr};
        detail::UniqueTask callback{};
    };

    using TimerMap = std::multimap<std::chrono::steady_clock::time_point, TimerEntry>;

    std::atomic<std::uint64_t> next_timer_id_{1};
    std::mutex timer_mutex_;
    std::condition_variable timer_cv_;
    TimerMap timers_;
    std::unordered_map<std::uint64_t, TimerMap::iterator> timer_index_;
    std::jthread timer_thread_;

    std::mutex routine_pool_mutex_;
    std::vector<std::unique_ptr<detail::Routine>> routine_pool_;

    static void routine_trampoline(void* arg) noexcept;

    detail::Routine* acquire_routine(detail::UniqueTask fn);
    void release_routine(detail::Routine* r) noexcept;
    void timer_loop();

public:
    /**
     * @brief Constructs and starts the M:N runtime with the given configuration.
     * @param cfg Configuration parameters (worker thread count, stack size, pool capacity).
     */
    explicit Runtime(Config cfg = {});

    /**
     * @brief Waits for all active routines to finish (`wait_idle()`) and cleanly joins all worker and timer threads.
     */
    ~Runtime();

    Runtime(const Runtime&) = delete;
    Runtime& operator=(const Runtime&) = delete;

    /**
     * @brief Schedules a `Ready` routine onto the current worker's local deque (if on a worker of this runtime)
     *        or onto the `Global Run Queue`, waking an idle worker thread if necessary.
     * @param r Pointer to the routine to schedule.
     */
    void schedule(detail::Routine* r) noexcept;

    /**
     * @brief Registers a cancellable timer deadline in the timer thread to wake routine `r`.
     * @return Unique non-zero timer ID that may be passed to `cancel_timer()`.
     */
    std::uint64_t schedule_timer(std::chrono::steady_clock::time_point deadline, detail::Routine* r);

    /**
     * @brief Registers a cancellable callback in the timer thread at `deadline`.
     * @return Unique non-zero timer ID that may be passed to `cancel_timer()`.
     */
    std::uint64_t schedule_timer_callback(std::chrono::steady_clock::time_point deadline, detail::UniqueTask cb);

    /**
     * @brief Cancels a pending timer by its `timer_id`, immediately reclaiming its memory from the timer map.
     * @return `true` if the timer was found and cancelled before firing, `false` if already fired or unknown.
     */
    bool cancel_timer(std::uint64_t timer_id) noexcept;

    /**
     * @brief Spawns a new green routine on this `Runtime` instance (supports move-only callables!).
     * @tparam F Callable type `void()`.
     * @param fn Callable task body.
     */
    template <typename F>
    void spawn_raw(F&& fn) {
        // Acquire routine BEFORE incrementing active_routines_ so std::bad_alloc never deadlocks wait_idle()!
        detail::Routine* r = acquire_routine(detail::UniqueTask(std::forward<F>(fn)));
        r->implicit_scope = detail::current_implicit_scope();
        active_routines_.fetch_add(1, std::memory_order_relaxed);
        schedule(r);
    }

    /**
     * @brief Spawns a green routine on this `Runtime` instance.
     * @tparam F Callable type `void()`.
     * @param fn Callable task body.
     */
    template <typename F>
    void go(F&& fn) {
        spawn_raw(std::forward<F>(fn));
    }

    /**
     * @brief Spawns `fn` as a green routine on this runtime and waits until it completes.
     *
     * Safe to call from BOTH external OS threads (`main()`) and inside existing green routines
     * (parks the calling green routine via `UniversalWaiter` instead of blocking the OS worker thread!).
     * Any uncaught exception thrown inside `fn` is captured into `std::exception_ptr` and rethrown.
     *
     * @tparam F Nullary callable type.
     * @param fn Entry function to execute inside the runtime.
     * @return The result returned by `fn()`.
     */
    template <std::invocable F>
    auto block_on(F&& fn) -> std::invoke_result_t<F>;

    /**
     * @brief Blocks the calling thread until all active routines in this runtime have finished executing.
     */
    void wait_idle() noexcept;

    /**
     * @brief Returns the number of OS worker threads (`Worker`) in this runtime.
     */
    [[nodiscard]] std::size_t worker_count() const noexcept {
        return config_.worker_count;
    }

    /**
     * @brief Returns the current number of active (unfinished) green routines.
     */
    [[nodiscard]] std::size_t active_routine_count() const noexcept {
        return active_routines_.load(std::memory_order_acquire);
    }

    /**
     * @brief Returns the current number of pending (uncancelled) timers in the timer thread.
     */
    [[nodiscard]] std::size_t pending_timer_count() noexcept {
        std::lock_guard lock(timer_mutex_);
        return timer_index_.size();
    }
};

/**
 * @brief Returns the lazy global default `Runtime` instance.
 *
 * Automatically initialized on the first call to `gorun::go(...)` and cleanly shut down at program exit.
 */
inline Runtime& default_runtime() {
    static Runtime instance{};
    return instance;
}

namespace detail {

/**
 * @class Worker
 * @brief OS worker thread combining the roles of `M` (OS thread) and `P` (logical processor) from Go's runtime.
 *
 * Owns a local `WorkStealingDeque<Routine*>`, a scheduler execution context `scheduler_ctx_`,
 * and implements the atomic `park_locked()` post-switch unlock protocol to prevent Lost Wakeups and stack races.
 */
class Worker {
    friend class ::gorun::Runtime;

    Runtime& runtime_;
    std::size_t index_;
    ExecutionContext scheduler_ctx_{};
    WorkStealingDeque<Routine*> local_queue_{256};

    Routine* current_routine_{nullptr};
    std::vector<std::unique_ptr<Routine>> local_free_routines_{}; ///< Per-worker lock-free routine cache (Go's `p.gFree`)
    SpinLock* pending_unlock_{nullptr};
    bool yield_requested_{false};
    bool finish_requested_{false};
    std::uint64_t tick_{0};
    std::mt19937 rng_;
#if defined(GORUN_HAS_ASAN)
    void* asan_scheduler_fake_stack_{nullptr};
    const void* asan_scheduler_bottom_{nullptr};
    std::size_t asan_scheduler_size_{0};
#endif

public:
    Worker(Runtime& rt, std::size_t idx)
        : runtime_(rt)
        , index_(idx)
        , rng_(static_cast<std::uint32_t>(idx * 2654435761u + 17u)) {}

    [[nodiscard]] Runtime& runtime() noexcept { return runtime_; }
    [[nodiscard]] std::size_t index() const noexcept { return index_; }
    [[nodiscard]] ExecutionContext& scheduler_context() noexcept { return scheduler_ctx_; }
    [[nodiscard]] std::vector<std::unique_ptr<Routine>>& local_free_routines() noexcept { return local_free_routines_; }

#if defined(GORUN_HAS_ASAN)
    void asan_before_switch_to_routine(Routine* r) noexcept {
        __sanitizer_start_switch_fiber(&asan_scheduler_fake_stack_, r->stack.stack_bottom, r->stack.usable_bytes);
    }

    void asan_after_switch_to_routine(Routine* r) noexcept {
        __sanitizer_finish_switch_fiber(r->asan_fake_stack, &asan_scheduler_bottom_, &asan_scheduler_size_);
    }

    void asan_before_switch_to_scheduler(Routine* r, bool is_finishing) noexcept {
        __sanitizer_start_switch_fiber(
            is_finishing ? nullptr : &r->asan_fake_stack,
            asan_scheduler_bottom_,
            asan_scheduler_size_
        );
    }

    void asan_after_switch_to_scheduler() noexcept {
        const void* dummy_bottom = nullptr;
        std::size_t dummy_size = 0;
        __sanitizer_finish_switch_fiber(asan_scheduler_fake_stack_, &dummy_bottom, &dummy_size);
    }
#endif

    /**
     * @brief Atomically transitions the current routine to `Parked`, switches CPU context to `scheduler_ctx_`,
     *        and releases `primitive_lock` **after** the routine's registers are safely saved on its stack.
     *
     * @note Direct C++ equivalent of Go's `gopark(unlockf, lock)` in `runtime/proc.go`.
     * Prevents both **Lost Wakeup** and **Premature Dual-Core Stack Execution**.
     *
     * @param primitive_lock Already-acquired spinlock of the synchronization primitive (`Channel`, `Mutex`, `WaitGroup`).
     */
    void park_locked(SpinLock& primitive_lock) noexcept {
        Routine* r = current_routine_;
        r->state = RoutineState::Parked;
        pending_unlock_ = &primitive_lock;
#if defined(GORUN_HAS_ASAN)
        asan_before_switch_to_scheduler(r, false);
#endif
        context_switch(&r->ctx, &scheduler_ctx_);
#if defined(GORUN_HAS_ASAN)
        current_worker()->asan_after_switch_to_routine(r);
#endif
    }

    /**
     * @brief Cooperatively yields execution back to the scheduler (`this_routine::yield()`), keeping the routine `Ready`.
     */
    void yield_current() noexcept {
        Routine* r = current_routine_;
        r->state = RoutineState::Ready;
        yield_requested_ = true;
#if defined(GORUN_HAS_ASAN)
        asan_before_switch_to_scheduler(r, false);
#endif
        context_switch(&r->ctx, &scheduler_ctx_);
#if defined(GORUN_HAS_ASAN)
        current_worker()->asan_after_switch_to_routine(r);
#endif
    }

    /**
     * @brief Marks the current routine as `Finished` and switches back to the worker scheduler loop for stack recycling.
     */
    void finish_current() noexcept {
        Routine* r = current_routine_;
        r->state = RoutineState::Finished;
        finish_requested_ = true;
#if defined(GORUN_HAS_ASAN)
        asan_before_switch_to_scheduler(r, false);
#endif
        context_switch(&r->ctx, &scheduler_ctx_);
#if defined(GORUN_HAS_ASAN)
        current_worker()->asan_after_switch_to_routine(r);
#endif
    }

    /**
     * @brief Transitions a parked routine `r` to `Ready` and schedules it for execution.
     */
    void unpark(Routine* r) noexcept {
        runtime_.schedule(r);
    }

    /**
     * @brief Selects the next runnable routine following Go's M:N scheduler polling order:
     * 1. Every 61st tick: poll `Global Run Queue` first to guarantee fairness and prevent starvation.
     * 2. Pop from local worker deque `local_queue_.pop_back()` (LIFO for L1/L2 cache locality).
     * 3. Pop from `Global Run Queue` (FIFO).
     * 4. Work-Stealing: attempt to steal half of the tasks (`steal_half_into`) from a randomly chosen victim worker.
     */
    Routine* find_runnable() {
        ++tick_;

        if ((tick_ % 61) == 0) {
            if (Routine* r = runtime_.global_queue_.pop_front()) {
                return r;
            }
        }

        if (Routine* r = local_queue_.pop_back()) {
            return r;
        }

        if (Routine* r = runtime_.global_queue_.pop_front()) {
            return r;
        }

        const std::size_t num_workers = runtime_.workers_.size();
        if (num_workers > 1) {
            std::size_t start = rng_() % num_workers;
            for (std::size_t i = 0; i < num_workers; ++i) {
                std::size_t victim_idx = (start + i) % num_workers;
                if (victim_idx == index_) continue;
                Worker* victim = runtime_.workers_[victim_idx].get();
                if (Routine* stolen = victim->local_queue_.steal_half_into(local_queue_)) {
                    return stolen;
                }
            }
        }

        return nullptr;
    }

    /**
     * @brief Main execution loop for the OS worker thread.
     */
    void run_loop() {
        tls_current_worker() = this;
        scheduler_ctx_.init_scheduler_for_current_thread();

        while (!runtime_.shutdown_.load(std::memory_order_acquire)) {
            Routine* r = find_runnable();
            if (!r) {
                // Short active spin before blocking the OS thread on condition_variable
                for (int spin = 0; spin < 64; ++spin) {
                    cpu_relax();
                    if ((r = find_runnable()) != nullptr) {
                        break;
                    }
                }
            }
            if (!r) {
                std::unique_lock lock(runtime_.sleep_mutex_);
                runtime_.sleeping_workers_.fetch_add(1, std::memory_order_seq_cst);
                if (runtime_.shutdown_.load(std::memory_order_acquire) ||
                    !runtime_.global_queue_.empty() || !local_queue_.empty()) {
                    runtime_.sleeping_workers_.fetch_sub(1, std::memory_order_seq_cst);
                    runtime_.waking_workers_.store(0, std::memory_order_release);
                    continue;
                }
                // Check if any sibling worker has stealable work before sleeping
                bool sibling_has_work = false;
                for (const auto& w : runtime_.workers_) {
                    if (w.get() != this && !w->local_queue_.empty()) {
                        sibling_has_work = true;
                        break;
                    }
                }
                if (sibling_has_work) {
                    runtime_.sleeping_workers_.fetch_sub(1, std::memory_order_seq_cst);
                    runtime_.waking_workers_.store(0, std::memory_order_release);
                    continue;
                }
                runtime_.sleep_cv_.wait_for(lock, std::chrono::milliseconds(50));
                runtime_.sleeping_workers_.fetch_sub(1, std::memory_order_seq_cst);
                runtime_.waking_workers_.store(0, std::memory_order_release);
                if (!runtime_.global_queue_.empty() &&
                    runtime_.sleeping_workers_.load(std::memory_order_relaxed) > 0) {
                    runtime_.sleep_cv_.notify_one();
                }
                continue;
            }

            current_routine_ = r;
            tls_current_routine() = r;
            r->state = RoutineState::Running;

#if defined(GORUN_HAS_ASAN)
            asan_before_switch_to_routine(r);
#endif
            context_switch(&scheduler_ctx_, &r->ctx);
#if defined(GORUN_HAS_ASAN)
            asan_after_switch_to_scheduler();
#endif

            current_routine_ = nullptr;
            tls_current_routine() = nullptr;

            if (pending_unlock_) {
                SpinLock* lk = std::exchange(pending_unlock_, nullptr);
                lk->unlock();
            }

            if (yield_requested_) {
                yield_requested_ = false;
                // Push to FIFO head so other queued local routines run before this routine (prevents LIFO starvation)
                local_queue_.push_front(r);
            } else if (finish_requested_) {
                finish_requested_ = false;
                runtime_.release_routine(r);
                if (runtime_.active_routines_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                    std::lock_guard lk(runtime_.sleep_mutex_);
                    runtime_.sleep_cv_.notify_all();
                }
            }
        }

        scheduler_ctx_.cleanup_scheduler_for_current_thread();
        tls_current_worker() = nullptr;
    }
};

inline void UniversalWaiter::park_current(SpinLock& primitive_lock) noexcept {
    if (Worker* w = current_worker()) {
        w->park_locked(primitive_lock);
    } else {
        primitive_lock.unlock();
        os_sem->acquire();
    }
}

inline void UniversalWaiter::unpark() noexcept {
    if (routine) {
        if (routine->owner_runtime) {
            routine->owner_runtime->schedule(routine);
        } else {
            default_runtime().schedule(routine);
        }
    } else if (os_sem) {
        os_sem->release();
    }
}

} // namespace detail

inline void Runtime::routine_trampoline(void* arg) noexcept {
    auto* r = static_cast<detail::Routine*>(arg);
#if defined(GORUN_HAS_ASAN)
    detail::current_worker()->asan_after_switch_to_routine(r);
#endif
    for (;;) {
        try {
            if (r->task) {
                r->task();
            }
        } catch (...) {
        }
        // Destroy captured closure & all RoutineLocal<T> instances immediately before recycling!
        r->task = nullptr;
        r->clear_locals();
        detail::current_worker()->finish_current();
    }
}

inline detail::Routine* Runtime::acquire_routine(detail::UniqueTask fn) {
    std::unique_ptr<detail::Routine> r;
    detail::Worker* w = detail::current_worker();
    if (w && &w->runtime() == this && !w->local_free_routines().empty()) {
        r = std::move(w->local_free_routines().back());
        w->local_free_routines().pop_back();
    } else {
        std::lock_guard lock(routine_pool_mutex_);
        if (!routine_pool_.empty()) {
            r = std::move(routine_pool_.back());
            routine_pool_.pop_back();
        }
    }

    if (!r) {
        r = std::make_unique<detail::Routine>();
        r->owner_runtime = this;
        r->stack = stack_pool_.acquire();
        r->ctx.make_routine_stack_context(r->stack, &Runtime::routine_trampoline, r.get());
    }

    r->id = next_routine_id_.fetch_add(1, std::memory_order_relaxed);
    r->state = detail::RoutineState::Ready;
    r->task = std::move(fn);
    r->stop_source = std::stop_source{};
    r->implicit_scope = nullptr;
    return r.release();
}

inline void Runtime::release_routine(detail::Routine* r) noexcept {
    std::unique_ptr<detail::Routine> ptr(r);
    detail::Worker* w = detail::current_worker();
    if (w && &w->runtime() == this && w->local_free_routines().size() < 64) {
        w->local_free_routines().push_back(std::move(ptr));
        return;
    }
    {
        std::lock_guard lock(routine_pool_mutex_);
        if (routine_pool_.size() < config_.max_pooled_stacks) {
            routine_pool_.push_back(std::move(ptr));
            return;
        }
    }
    stack_pool_.release(ptr->stack);
}

inline Runtime::Runtime(Config cfg)
    : config_(cfg)
    , stack_pool_(cfg.default_stack_size, cfg.max_pooled_stacks) {
    if (config_.worker_count == 0) {
        config_.worker_count = 1;
    }

    workers_.reserve(config_.worker_count);
    for (std::size_t i = 0; i < config_.worker_count; ++i) {
        workers_.push_back(std::make_unique<detail::Worker>(*this, i));
    }

    timer_thread_ = std::jthread([this]() { timer_loop(); });

    worker_threads_.reserve(config_.worker_count);
    for (std::size_t i = 0; i < config_.worker_count; ++i) {
        detail::Worker* w = workers_[i].get();
        worker_threads_.emplace_back([w]() { w->run_loop(); });
    }
}

inline Runtime::~Runtime() {
    wait_idle();

    shutdown_.store(true, std::memory_order_release);
    {
        std::lock_guard lk(sleep_mutex_);
        sleep_cv_.notify_all();
    }
    {
        std::lock_guard lk(timer_mutex_);
        timer_cv_.notify_all();
    }

    for (auto& t : worker_threads_) {
        if (t.joinable()) {
            t.join();
        }
    }
    if (timer_thread_.joinable()) {
        timer_thread_.join();
    }

    std::lock_guard lock(routine_pool_mutex_);
    for (auto& w : workers_) {
        for (auto& r : w->local_free_routines()) {
            stack_pool_.release(r->stack);
        }
        w->local_free_routines().clear();
    }
    for (auto& r : routine_pool_) {
        stack_pool_.release(r->stack);
    }
    routine_pool_.clear();
}

inline void Runtime::schedule(detail::Routine* r) noexcept {
    r->state = detail::RoutineState::Ready;
    bool should_wake = false;
    detail::Worker* w = detail::current_worker();
    if (w && &w->runtime() == this) {
        // Only wake an idle sibling core if this worker now has >=2 local runnable routines;
        // keeping single-task producer-consumer handoffs in run_next_ preserves L1 cache locality.
        should_wake = w->local_queue_.push_run_next(r);
    } else {
        global_queue_.push_back(r);
        should_wake = true;
    }

    if (should_wake) {
        // Full fence pairs with sleeping_workers_.fetch_add(seq_cst) in Worker::run_loop() to prevent lost wakeups!
        std::atomic_thread_fence(std::memory_order_seq_cst);
        if (sleeping_workers_.load(std::memory_order_seq_cst) > 0 &&
            waking_workers_.exchange(1, std::memory_order_acq_rel) == 0) {
            std::lock_guard lk(sleep_mutex_);
            sleep_cv_.notify_one();
        }
    }
}

inline std::uint64_t Runtime::schedule_timer(std::chrono::steady_clock::time_point deadline, detail::Routine* r) {
    std::uint64_t id = next_timer_id_.fetch_add(1, std::memory_order_relaxed);
    bool is_earliest = false;
    {
        std::lock_guard lock(timer_mutex_);
        auto it = timers_.emplace(deadline, TimerEntry{.id = id, .routine = r, .callback = nullptr});
        timer_index_.emplace(id, it);
        is_earliest = (it == timers_.begin());
    }
    if (is_earliest) {
        timer_cv_.notify_one();
    }
    return id;
}

inline std::uint64_t Runtime::schedule_timer_callback(std::chrono::steady_clock::time_point deadline, detail::UniqueTask cb) {
    std::uint64_t id = next_timer_id_.fetch_add(1, std::memory_order_relaxed);
    bool is_earliest = false;
    {
        std::lock_guard lock(timer_mutex_);
        auto it = timers_.emplace(deadline, TimerEntry{.id = id, .routine = nullptr, .callback = std::move(cb)});
        timer_index_.emplace(id, it);
        is_earliest = (it == timers_.begin());
    }
    if (is_earliest) {
        timer_cv_.notify_one();
    }
    return id;
}

inline bool Runtime::cancel_timer(std::uint64_t timer_id) noexcept {
    if (timer_id == 0) {
        return false;
    }
    std::lock_guard lock(timer_mutex_);
    auto idx_it = timer_index_.find(timer_id);
    if (idx_it == timer_index_.end()) {
        return false;
    }
    timers_.erase(idx_it->second);
    timer_index_.erase(idx_it);
    return true;
}

inline void Runtime::timer_loop() {
    std::unique_lock lock(timer_mutex_);
    while (!shutdown_.load(std::memory_order_acquire)) {
        if (timers_.empty()) {
            timer_cv_.wait(lock, [this]() {
                return shutdown_.load(std::memory_order_acquire) || !timers_.empty();
            });
            continue;
        }

        auto now = std::chrono::steady_clock::now();
        auto earliest = timers_.begin();
        if (earliest->first <= now) {
            TimerEntry entry = std::move(earliest->second);
            timer_index_.erase(entry.id);
            timers_.erase(earliest);
            lock.unlock();

            if (entry.routine) {
                schedule(entry.routine);
            } else if (entry.callback) {
                entry.callback();
            }

            lock.lock();
        } else {
            auto next_deadline = earliest->first;
            timer_cv_.wait_until(lock, next_deadline);
        }
    }
}

inline void Runtime::wait_idle() noexcept {
    std::unique_lock lock(sleep_mutex_);
    sleep_cv_.wait(lock, [this]() {
        return active_routines_.load(std::memory_order_acquire) == 0;
    });
}

template <std::invocable F>
auto Runtime::block_on(F&& fn) -> std::invoke_result_t<F> {
    using R = std::invoke_result_t<F>;
    detail::SpinLock done_lock;
    bool is_done = false;
    detail::UniversalWaiter* waiter_ptr = nullptr;
    std::exception_ptr exc{nullptr};

    auto notify_done = [&]() noexcept {
        done_lock.lock();
        is_done = true;
        detail::UniversalWaiter* w = waiter_ptr;
        done_lock.unlock();
        if (w) {
            w->unpark();
        }
    };

    auto wait_done = [&]() noexcept {
        done_lock.lock();
        if (!is_done) {
            std::binary_semaphore fallback_sem{0};
            auto waiter = detail::UniversalWaiter::make_for_current(fallback_sem);
            waiter_ptr = &waiter;
            waiter.park_current(done_lock);
        } else {
            done_lock.unlock();
        }
    };

    if constexpr (std::is_void_v<R>) {
        spawn_raw([&, f = std::forward<F>(fn)]() mutable {
            try {
                std::invoke(f);
            } catch (...) {
                exc = std::current_exception();
            }
            notify_done();
        });
        wait_done();
        if (exc) {
            std::rethrow_exception(exc);
        }
    } else {
        std::optional<R> result;
        spawn_raw([&, f = std::forward<F>(fn)]() mutable {
            try {
                result.emplace(std::invoke(f));
            } catch (...) {
                exc = std::current_exception();
            }
            notify_done();
        });
        wait_done();
        if (exc) {
            std::rethrow_exception(exc);
        }
        return std::move(*result);
    }
}

/**
 * @namespace gorun::this_routine
 * @brief Execution control functions for the currently running green routine (analogous to `std::this_thread`).
 */
namespace this_routine {

/**
 * @brief Cooperatively yields the processor to other runnable green routines (analogous to `runtime.Gosched()` in Go).
 */
inline void yield() noexcept {
    if (detail::Worker* w = detail::current_worker()) {
        w->yield_current();
    } else {
        std::this_thread::yield();
    }
}

/**
 * @brief Suspends the current green routine for `duration` without blocking the underlying OS worker thread
 *        (analogous to `time.Sleep(d)` in Go).
 *
 * @param duration Sleep duration (e.g. `std::chrono::milliseconds(50)` or `50ms`).
 */
template <typename Rep, typename Period>
void sleep_for(std::chrono::duration<Rep, Period> duration) {
    if (duration <= std::chrono::duration<Rep, Period>::zero()) {
        return;
    }
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::duration_cast<std::chrono::steady_clock::duration>(duration);

    if (detail::Worker* w = detail::current_worker()) {
        detail::Routine* r = detail::current_routine();
        detail::SpinLock timer_park_lock;
        timer_park_lock.lock();
        Runtime* rt = &w->runtime();
        rt->schedule_timer_callback(deadline, [rt, r, &timer_park_lock]() {
            timer_park_lock.lock();
            timer_park_lock.unlock();
            rt->schedule(r);
        });
        w->park_locked(timer_park_lock);
    } else {
        std::this_thread::sleep_for(duration);
    }
}

/**
 * @brief Returns the unique 64-bit ID of the current green routine (or `0` if called from an external OS thread).
 */
[[nodiscard]] inline std::uint64_t id() noexcept {
    if (detail::Routine* r = detail::current_routine()) {
        return r->id;
    }
    return 0;
}

/**
 * @brief Returns the cooperative cancellation token (`std::stop_token`) associated with the current routine.
 */
[[nodiscard]] inline std::stop_token get_stop_token() noexcept {
    if (detail::Routine* r = detail::current_routine()) {
        return r->stop_source.get_token();
    }
    return std::stop_token{};
}

} // namespace this_routine

/**
 * @brief Shorthand alias for `gorun::this_routine::yield()`.
 */
inline void yield() noexcept {
    this_routine::yield();
}

/**
 * @brief Shorthand alias for `gorun::this_routine::sleep_for(duration)`.
 */
template <typename Rep, typename Period>
void sleep_for(std::chrono::duration<Rep, Period> duration) {
    this_routine::sleep_for(duration);
}

/**
 * @class RoutineLocal
 * @brief Lock-free, zero-leak routine-local storage container isolated per green routine
 *        (safe replacement for `thread_local`).
 *
 * Values are stored directly on the owning `Routine` and automatically destroyed the instant
 * the routine finishes — preventing unbounded memory growth in long-running web servers.
 * External OS threads automatically fall back to thread-safe `thread_local` storage.
 *
 * @tparam T Default-constructible value type stored per routine.
 */
template <typename T>
class RoutineLocal {
    static std::atomic<std::uint64_t>& next_key_id() noexcept {
        static std::atomic<std::uint64_t> counter{1};
        return counter;
    }

    static void destroy_value(void* ptr) noexcept {
        delete static_cast<T*>(ptr);
    }

    const std::uint64_t key_id_;

public:
    RoutineLocal() noexcept
        : key_id_(next_key_id().fetch_add(1, std::memory_order_relaxed)) {}

    RoutineLocal(const RoutineLocal&) = delete;
    RoutineLocal& operator=(const RoutineLocal&) = delete;

    /**
     * @brief Returns a reference to the `T` instance owned by the current green routine (lock-free!)
     *        or the current external OS thread.
     */
    T& get() {
        if (detail::Routine* r = detail::current_routine()) {
            auto& slot = r->routine_locals[key_id_];
            if (!slot.ptr) {
                slot.ptr = new T{};
                slot.deleter = &destroy_value;
            }
            return *static_cast<T*>(slot.ptr);
        }

        // External OS thread fallback: strictly isolated per OS thread (zero data race!)
        thread_local std::unordered_map<std::uint64_t, detail::Routine::LocalSlot> os_thread_locals;
        auto& slot = os_thread_locals[key_id_];
        if (!slot.ptr) {
            slot.ptr = new T{};
            slot.deleter = &destroy_value;
        }
        return *static_cast<T*>(slot.ptr);
    }

    T& operator*() { return get(); }
    T* operator->() { return &get(); }
};

} // namespace gorun
// ---- end gorun/runtime.hpp ----
// ---- begin gorun/sync.hpp ----

/**
 * @file sync.hpp
 * @brief Green-thread synchronization primitives with zero OS-thread blocking:
 *        `Mutex`, `CondVar`, `WaitGroup`, `Semaphore`, `Once`, and `RwMutex`.
 */

#include <cstddef>
#include <cstdint>
#include <exception>
#include <mutex>
#include <optional>
#include <semaphore>
#include <stdexcept>
#include <utility>



namespace gorun {

/**
 * @class Mutex
 * @brief Non-OS-blocking mutual exclusion lock for green routines (analogous to `sync.Mutex` in Go).
 *
 * Fully satisfies the C++ `BasicLockable` and `Lockable` named requirements, making it directly compatible
 * with standard RAII wrappers: `std::lock_guard<gorun::Mutex>`, `std::unique_lock<gorun::Mutex>`, and `std::scoped_lock`.
 */
class Mutex {
    detail::SpinLock wait_lock_;
    bool is_locked_{false};
    detail::IntrusiveQueue<detail::UniversalWaiter> waiters_;

public:
    constexpr Mutex() noexcept = default;
    Mutex(const Mutex&) = delete;
    Mutex& operator=(const Mutex&) = delete;

    /**
     * @brief Attempts to acquire the mutex without suspending the calling routine.
     * @return `true` if the lock was acquired, `false` if already held.
     */
    [[nodiscard]] bool try_lock() noexcept {
        wait_lock_.lock();
        if (!is_locked_) {
            is_locked_ = true;
            wait_lock_.unlock();
            return true;
        }
        wait_lock_.unlock();
        return false;
    }

    /**
     * @brief Acquires the mutex. If contended, parks the current green routine without blocking the OS worker thread.
     */
    void lock() noexcept {
        for (int spin = 0; spin < 32; ++spin) {
            if (try_lock()) {
                return;
            }
            detail::cpu_relax();
        }

        wait_lock_.lock();
        if (!is_locked_) {
            is_locked_ = true;
            wait_lock_.unlock();
            return;
        }

        std::binary_semaphore fallback_sem{0};
        auto waiter = detail::UniversalWaiter::make_for_current(fallback_sem);
        waiters_.push_back(&waiter);

        waiter.park_current(wait_lock_);
    }

    /**
     * @brief Releases the mutex. If routines are waiting, transfers ownership directly (Direct Handoff FIFO)
     *        to the next waiter and schedules it.
     */
    void unlock() noexcept {
        wait_lock_.lock();
        if (!is_locked_) {
            wait_lock_.unlock();
            return;
        }
        if (auto* next = waiters_.pop_front()) {
            wait_lock_.unlock();
            next->unpark();
        } else {
            is_locked_ = false;
            wait_lock_.unlock();
        }
    }
};

/**
 * @class CondVar
 * @brief Condition variable for green routines (analogous to `sync.Cond` in Go and `std::condition_variable` in C++).
 */
class CondVar {
    detail::SpinLock lock_;
    detail::IntrusiveQueue<detail::UniversalWaiter> waiters_;

public:
    constexpr CondVar() noexcept = default;
    CondVar(const CondVar&) = delete;
    CondVar& operator=(const CondVar&) = delete;

    void wait(std::unique_lock<Mutex>& user_lock) noexcept {
        std::binary_semaphore fallback_sem{0};
        auto waiter = detail::UniversalWaiter::make_for_current(fallback_sem);

        lock_.lock();
        waiters_.push_back(&waiter);
        user_lock.unlock();

        waiter.park_current(lock_);

        user_lock.lock();
    }

    template <typename Predicate>
    void wait(std::unique_lock<Mutex>& user_lock, Predicate pred) {
        while (!pred()) {
            wait(user_lock);
        }
    }

    void notify_one() noexcept {
        lock_.lock();
        auto* w = waiters_.pop_front();
        lock_.unlock();

        if (w) {
            w->unpark();
        }
    }

    void notify_all() noexcept {
        detail::IntrusiveQueue<detail::UniversalWaiter> to_wake;
        lock_.lock();
        to_wake = std::move(waiters_);
        lock_.unlock();

        while (auto* w = to_wake.pop_front()) {
            w->unpark();
        }
    }
};

/**
 * @class WaitGroup
 * @brief Task synchronization barrier (direct equivalent of `sync.WaitGroup` in Go).
 */
class WaitGroup {
    detail::SpinLock lock_;
    std::int64_t counter_{0};
    detail::IntrusiveQueue<detail::UniversalWaiter> waiters_;

public:
    constexpr WaitGroup() noexcept = default;
    WaitGroup(const WaitGroup&) = delete;
    WaitGroup& operator=(const WaitGroup&) = delete;

    void add(std::int64_t delta = 1) {
        lock_.lock();
        counter_ += delta;
        if (counter_ < 0) {
            counter_ -= delta;
            lock_.unlock();
            throw std::logic_error("gorun::WaitGroup: negative counter");
        }
        if (counter_ > 0) {
            lock_.unlock();
            return;
        }

        auto to_wake = std::move(waiters_);
        lock_.unlock();

        while (auto* w = to_wake.pop_front()) {
            w->unpark();
        }
    }

    void done() {
        add(-1);
    }

    void wait() noexcept {
        lock_.lock();
        if (counter_ == 0) {
            lock_.unlock();
            return;
        }

        std::binary_semaphore fallback_sem{0};
        auto waiter = detail::UniversalWaiter::make_for_current(fallback_sem);
        waiters_.push_back(&waiter);
        waiter.park_current(lock_);
    }
};

/**
 * @class Semaphore
 * @brief Counting semaphore for bounding concurrency among green routines without blocking OS worker threads.
 */
class Semaphore {
    detail::SpinLock lock_;
    std::ptrdiff_t permits_;
    detail::IntrusiveQueue<detail::UniversalWaiter> waiters_;

public:
    explicit constexpr Semaphore(std::ptrdiff_t initial_permits) noexcept
        : permits_(initial_permits > 0 ? initial_permits : 0) {}

    Semaphore(const Semaphore&) = delete;
    Semaphore& operator=(const Semaphore&) = delete;

    [[nodiscard]] bool try_acquire() noexcept {
        lock_.lock();
        if (permits_ > 0) {
            --permits_;
            lock_.unlock();
            return true;
        }
        lock_.unlock();
        return false;
    }

    void acquire() noexcept {
        lock_.lock();
        if (permits_ > 0) {
            --permits_;
            lock_.unlock();
            return;
        }

        std::binary_semaphore fallback_sem{0};
        auto waiter = detail::UniversalWaiter::make_for_current(fallback_sem);
        waiters_.push_back(&waiter);
        waiter.park_current(lock_);
    }

    void release(std::ptrdiff_t update = 1) noexcept {
        if (update <= 0) {
            return;
        }
        detail::IntrusiveQueue<detail::UniversalWaiter> to_wake;
        lock_.lock();
        for (std::ptrdiff_t i = 0; i < update; ++i) {
            if (auto* w = waiters_.pop_front()) {
                to_wake.push_back(w);
            } else {
                ++permits_;
            }
        }
        lock_.unlock();

        while (auto* w = to_wake.pop_front()) {
            w->unpark();
        }
    }
};

/**
 * @class Once
 * @brief One-time initialization primitive for green routines (analogous to `sync.Once` in Go).
 */
class Once {
    Mutex mutex_;
    std::atomic<bool> done_{false};

public:
    constexpr Once() noexcept = default;
    Once(const Once&) = delete;
    Once& operator=(const Once&) = delete;

    template <typename Callable, typename... Args>
    void call(Callable&& fn, Args&&... args) {
        if (done_.load(std::memory_order_acquire)) {
            return;
        }
        std::lock_guard guard(mutex_);
        if (!done_.load(std::memory_order_relaxed)) {
            std::invoke(std::forward<Callable>(fn), std::forward<Args>(args)...);
            done_.store(true, std::memory_order_release);
        }
    }
};

template <typename Callable, typename... Args>
void call_once(Once& once_flag, Callable&& fn, Args&&... args) {
    once_flag.call(std::forward<Callable>(fn), std::forward<Args>(args)...);
}

/**
 * @class RwMutex
 * @brief Writer-preferring Read-Write mutex for green routines (compatible with `std::shared_lock` & `std::unique_lock`).
 */
class RwMutex {
    detail::SpinLock lock_;
    std::size_t active_readers_{0};
    bool active_writer_{false};
    detail::IntrusiveQueue<detail::UniversalWaiter> waiting_writers_;
    detail::IntrusiveQueue<detail::UniversalWaiter> waiting_readers_;

public:
    constexpr RwMutex() noexcept = default;
    RwMutex(const RwMutex&) = delete;
    RwMutex& operator=(const RwMutex&) = delete;

    void lock() noexcept {
        lock_.lock();
        if (!active_writer_ && active_readers_ == 0) {
            active_writer_ = true;
            lock_.unlock();
            return;
        }

        std::binary_semaphore fallback_sem{0};
        auto waiter = detail::UniversalWaiter::make_for_current(fallback_sem);
        waiting_writers_.push_back(&waiter);
        waiter.park_current(lock_);
    }

    [[nodiscard]] bool try_lock() noexcept {
        lock_.lock();
        if (!active_writer_ && active_readers_ == 0) {
            active_writer_ = true;
            lock_.unlock();
            return true;
        }
        lock_.unlock();
        return false;
    }

    void unlock() noexcept {
        detail::IntrusiveQueue<detail::UniversalWaiter> readers_to_wake;
        detail::UniversalWaiter* writer_to_wake = nullptr;

        lock_.lock();
        if (!active_writer_) {
            lock_.unlock();
            return;
        }
        active_writer_ = false;
        if (!waiting_writers_.empty()) {
            active_writer_ = true;
            writer_to_wake = waiting_writers_.pop_front();
        } else {
            while (auto* r = waiting_readers_.pop_front()) {
                ++active_readers_;
                readers_to_wake.push_back(r);
            }
        }
        lock_.unlock();

        if (writer_to_wake) {
            writer_to_wake->unpark();
        } else {
            while (auto* r = readers_to_wake.pop_front()) {
                r->unpark();
            }
        }
    }

    void lock_shared() noexcept {
        lock_.lock();
        if (!active_writer_ && waiting_writers_.empty()) {
            ++active_readers_;
            lock_.unlock();
            return;
        }

        std::binary_semaphore fallback_sem{0};
        auto waiter = detail::UniversalWaiter::make_for_current(fallback_sem);
        waiting_readers_.push_back(&waiter);
        waiter.park_current(lock_);
    }

    [[nodiscard]] bool try_lock_shared() noexcept {
        lock_.lock();
        if (!active_writer_ && waiting_writers_.empty()) {
            ++active_readers_;
            lock_.unlock();
            return true;
        }
        lock_.unlock();
        return false;
    }

    void unlock_shared() noexcept {
        detail::UniversalWaiter* writer_to_wake = nullptr;
        lock_.lock();
        if (active_readers_ == 0) {
            lock_.unlock();
            return;
        }
        --active_readers_;
        if (active_readers_ == 0 && !waiting_writers_.empty()) {
            active_writer_ = true;
            writer_to_wake = waiting_writers_.pop_front();
        }
        lock_.unlock();

        if (writer_to_wake) {
            writer_to_wake->unpark();
        }
    }
};

} // namespace gorun
// ---- end gorun/sync.hpp ----
// ---- begin gorun/channel.hpp ----

/**
 * @file channel.hpp
 * @brief Typed message-passing channels `gorun::Channel<T>` (buffered and unbuffered rendezvous)
 *        and the zero-busy-spin parked `gorun::select(...)` multiplexer.
 */

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <semaphore>
#include <tuple>
#include <utility>



namespace gorun {

namespace detail {

/**
 * @struct ChannelWaiter
 * @brief Intrusive stack-allocated waiter node for a blocked sender or receiver on a `Channel<T>` (Go's `sudog`).
 *
 * Holds a direct pointer (`data_slot`) to the `T` or `std::optional<T>` variable on the parked routine's stack frame,
 * enabling zero-allocation **Direct Stack-to-Stack Handoff** via `std::move`.
 * Also supports true parked `gorun::select(...)` via `select_winner` and `select_park_lock`.
 */
template <typename T>
struct ChannelWaiter {
    UniversalWaiter waiter{};
    void* data_slot{nullptr};
    bool woken_by_close{false};
    bool queued{false};

    // Multi-channel select() coordination
    std::atomic<int>* select_winner{nullptr};
    SpinLock* select_park_lock{nullptr};
    int select_case_index{0};

    ChannelWaiter* prev{nullptr};
    ChannelWaiter* next{nullptr};

    bool try_claim_for_select() noexcept {
        if (!select_winner) {
            return true;
        }
        int expected = -1;
        return select_winner->compare_exchange_strong(expected, select_case_index, std::memory_order_acq_rel);
    }

    // Wake helper that respects select_park_lock synchronization (guarantees context_switch finished before unpark!)
    void wake_after_unlock() noexcept {
        UniversalWaiter w = waiter;
        SpinLock* pl = select_park_lock;
        if (pl) {
            pl->lock();
            pl->unlock();
        }
        w.unpark();
    }
};

struct SelectWakeTarget {
    UniversalWaiter waiter{};
    SpinLock* select_park_lock{nullptr};

    void wake() noexcept {
        if (select_park_lock) {
            select_park_lock->lock();
            select_park_lock->unlock();
        }
        waiter.unpark();
    }
};

} // namespace detail

template <typename T, typename Callback>
struct RecvCase;

template <typename T, typename Callback>
struct SendCase;

/**
 * @class Channel
 * @brief Typed MPMC (Multi-Producer Multi-Consumer) channel modeled directly on Go's `chan T` (`runtime.hchan`).
 *
 * Exception-safe (`std::unique_lock` around all move constructions), zero-allocation on `send`/`recv`/`close`,
 * and supports true parked `gorun::select(...)` with zero busy-spinning.
 *
 * @tparam T Payload type transmitted across the channel (supports move-only types such as `std::unique_ptr<U>`).
 */
template <typename T>
class Channel {
private:
    template <typename U, typename Cb>
    friend struct RecvCase;
    template <typename U, typename Cb>
    friend struct SendCase;

    detail::SpinLock lock_;
    const std::size_t capacity_;
    std::unique_ptr<std::optional<T>[]> buffer_;
    std::size_t head_{0};
    std::size_t tail_{0};
    std::size_t count_{0};

    detail::IntrusiveQueue<detail::ChannelWaiter<T>> senders_;
    detail::IntrusiveQueue<detail::ChannelWaiter<T>> receivers_;
    bool closed_{false};

    detail::ChannelWaiter<T>* pop_valid_receiver() noexcept {
        while (auto* r = receivers_.pop_front()) {
            r->queued = false;
            if (r->try_claim_for_select()) {
                return r;
            }
        }
        return nullptr;
    }

    detail::ChannelWaiter<T>* pop_valid_sender() noexcept {
        while (auto* s = senders_.pop_front()) {
            s->queued = false;
            if (s->try_claim_for_select()) {
                return s;
            }
        }
        return nullptr;
    }

    // Internal lock access for deadlock-free sorted multi-channel select() locking
    detail::SpinLock* internal_lock_ptr() noexcept { return &lock_; }

    // Called while lock_ is ALREADY HELD by select()
    bool try_recv_locked(std::optional<T>& out_val, bool& is_closed_empty_out,
                         std::optional<detail::SelectWakeTarget>& wake_out) {
        is_closed_empty_out = false;
        if (count_ > 0) {
            out_val = std::move(buffer_[head_]);
            buffer_[head_].reset();
            head_ = (head_ + 1) % capacity_;
            --count_;

            if (auto* sender = pop_valid_sender()) {
                T* src = static_cast<T*>(sender->data_slot);
                try {
                    buffer_[tail_].emplace(std::move(*src));
                } catch (...) {
                    sender->queued = true;
                    senders_.push_back(sender);
                    throw;
                }
                tail_ = (tail_ + 1) % capacity_;
                ++count_;
                wake_out = detail::SelectWakeTarget{sender->waiter, sender->select_park_lock};
            }
            return true;
        }

        if (auto* sender = pop_valid_sender()) {
            T* src = static_cast<T*>(sender->data_slot);
            try {
                out_val.emplace(std::move(*src));
            } catch (...) {
                sender->queued = true;
                senders_.push_back(sender);
                throw;
            }
            wake_out = detail::SelectWakeTarget{sender->waiter, sender->select_park_lock};
            return true;
        }

        if (closed_) {
            is_closed_empty_out = true;
            out_val = std::nullopt;
            return true;
        }

        return false;
    }

    // Called while lock_ is ALREADY HELD by select()
    bool try_send_locked(T& value, bool& is_closed_out,
                         std::optional<detail::SelectWakeTarget>& wake_out) {
        if (closed_) {
            is_closed_out = true;
            return false;
        }
        is_closed_out = false;

        if (auto* receiver = pop_valid_receiver()) {
            auto* dst = static_cast<std::optional<T>*>(receiver->data_slot);
            try {
                dst->emplace(std::move(value));
            } catch (...) {
                receiver->queued = true;
                receivers_.push_back(receiver);
                throw;
            }
            wake_out = detail::SelectWakeTarget{receiver->waiter, receiver->select_park_lock};
            return true;
        }

        if (count_ < capacity_) {
            buffer_[tail_].emplace(std::move(value));
            tail_ = (tail_ + 1) % capacity_;
            ++count_;
            return true;
        }

        return false;
    }

    void enqueue_receiver_locked(detail::ChannelWaiter<T>* node) noexcept {
        node->queued = true;
        receivers_.push_back(node);
    }

    void enqueue_sender_locked(detail::ChannelWaiter<T>* node) noexcept {
        node->queued = true;
        senders_.push_back(node);
    }

    void remove_receiver(detail::ChannelWaiter<T>* node) noexcept {
        std::lock_guard guard(lock_);
        if (node->queued) {
            receivers_.remove(node);
            node->queued = false;
        }
    }

    void remove_sender(detail::ChannelWaiter<T>* node) noexcept {
        std::lock_guard guard(lock_);
        if (node->queued) {
            senders_.remove(node);
            node->queued = false;
        }
    }

public:
    /**
     * @brief Constructs a channel with the given buffer capacity.
     * @param capacity Buffer capacity (`0` for an unbuffered synchronous rendezvous channel).
     */
    explicit Channel(std::size_t capacity = 0)
        : capacity_(capacity)
        , buffer_(capacity > 0 ? std::make_unique<std::optional<T>[]>(capacity) : nullptr) {}

    /**
     * @brief Closes the channel and wakes any remaining parked readers or writers upon destruction.
     */
    ~Channel() {
        close();
    }

    Channel(const Channel&) = delete;
    Channel& operator=(const Channel&) = delete;

    /**
     * @brief Non-blocking attempt to send a value into the channel.
     * @param value Reference to the value to send (moved only if the send succeeds).
     * @param is_closed_out Set to `true` if the channel is already closed.
     * @return `true` if the value was delivered to a waiting receiver or placed in the buffer.
     */
    bool try_send(T& value, bool& is_closed_out) {
        std::optional<detail::SelectWakeTarget> to_wake;
        bool ok = false;
        {
            std::lock_guard guard(lock_);
            ok = try_send_locked(value, is_closed_out, to_wake);
        }
        if (to_wake) {
            to_wake->wake();
        }
        return ok;
    }

    /**
     * @brief Non-blocking attempt to receive a value from the channel.
     * @param out_val Destination `std::optional<T>` receiving the value.
     * @param is_closed_empty_out Set to `true` if the channel is closed and drained.
     * @return `true` if a value was received or the channel is closed and empty.
     */
    bool try_recv(std::optional<T>& out_val, bool& is_closed_empty_out) {
        std::optional<detail::SelectWakeTarget> to_wake;
        bool ok = false;
        {
            std::lock_guard guard(lock_);
            ok = try_recv_locked(out_val, is_closed_empty_out, to_wake);
        }
        if (to_wake) {
            to_wake->wake();
        }
        return ok;
    }

    /**
     * @brief Sends `value` into the channel (`ch <- value` in Go).
     *
     * Exception-safe: if moving `T` throws, the internal spinlock is released via RAII and waiting
     * receivers are never leaked.
     *
     * @param value Value to transmit (moved into the receiver or buffer).
     * @return `true` if delivered; `false` if the channel is closed.
     */
    bool send(T value) {
        std::unique_lock<detail::SpinLock> guard(lock_);

        std::optional<detail::SelectWakeTarget> to_wake;
        bool is_closed = false;
        if (try_send_locked(value, is_closed, to_wake)) {
            guard.unlock();
            if (to_wake) {
                to_wake->wake();
            }
            return true;
        }
        if (is_closed) {
            return false;
        }

        std::binary_semaphore fallback_sem{0};
        detail::ChannelWaiter<T> node{
            .waiter = detail::UniversalWaiter::make_for_current(fallback_sem),
            .data_slot = &value,
            .woken_by_close = false,
            .queued = true
        };

        senders_.push_back(&node);
        guard.release(); // Ownership of lock_ transfers atomically to park_current()!
        node.waiter.park_current(lock_);

        return !node.woken_by_close;
    }

    /**
     * @brief Receives the next value from the channel (`v, ok := <-ch` in Go).
     *
     * @return `std::optional<T>` containing the received item, or `std::nullopt` once the channel is closed and drained.
     */
    std::optional<T> recv() {
        std::unique_lock<detail::SpinLock> guard(lock_);

        std::optional<T> result;
        std::optional<detail::SelectWakeTarget> to_wake;
        bool closed_empty = false;
        if (try_recv_locked(result, closed_empty, to_wake)) {
            guard.unlock();
            if (to_wake) {
                to_wake->wake();
            }
            return result;
        }

        std::optional<T> result_slot;
        std::binary_semaphore fallback_sem{0};
        detail::ChannelWaiter<T> node{
            .waiter = detail::UniversalWaiter::make_for_current(fallback_sem),
            .data_slot = &result_slot,
            .woken_by_close = false,
            .queued = true
        };

        receivers_.push_back(&node);
        guard.release(); // Ownership of lock_ transfers atomically to park_current()!
        node.waiter.park_current(lock_);

        if (node.woken_by_close) {
            return std::nullopt;
        }
        return result_slot;
    }

    /**
     * @brief Closes the channel (`close(ch)` in Go) and wakes all parked readers and writers.
     *
     * Zero heap allocations (`noexcept`-safe: cannot trigger `std::terminate()` on OOM).
     */
    void close() noexcept {
        detail::IntrusiveQueue<detail::ChannelWaiter<T>> to_wake_receivers;
        detail::IntrusiveQueue<detail::ChannelWaiter<T>> to_wake_senders;

        {
            std::lock_guard guard(lock_);
            if (closed_) {
                return;
            }
            closed_ = true;

            while (auto* r = receivers_.pop_front()) {
                r->queued = false;
                if (r->try_claim_for_select()) {
                    r->woken_by_close = true;
                    to_wake_receivers.push_back(r);
                }
            }
            while (auto* s = senders_.pop_front()) {
                s->queued = false;
                if (s->try_claim_for_select()) {
                    s->woken_by_close = true;
                    to_wake_senders.push_back(s);
                }
            }
        }

        while (auto* r = to_wake_receivers.pop_front()) {
            r->wake_after_unlock();
        }
        while (auto* s = to_wake_senders.pop_front()) {
            s->wake_after_unlock();
        }
    }

    [[nodiscard]] bool is_closed() noexcept {
        std::lock_guard g(lock_);
        return closed_;
    }

    [[nodiscard]] std::size_t size() noexcept {
        std::lock_guard g(lock_);
        return count_;
    }

    [[nodiscard]] std::size_t capacity() const noexcept {
        return capacity_;
    }

    template <typename Callback>
    auto on_recv(Callback&& cb);

    template <typename Callback>
    auto on_send(T val, Callback&& cb);
};

template <typename T, typename Callback>
struct RecvCase {
    static constexpr bool kHandlesClose = std::is_invocable_v<Callback, std::optional<T>>;

    Channel<T>* channel;
    Callback callback;
    std::optional<T> result_slot{};
    detail::ChannelWaiter<T> waiter_node{};
    bool closed_skipped{false};

    detail::SpinLock* lock_ptr() noexcept { return channel->internal_lock_ptr(); }

    // Non-blocking fast path (locks channel itself)
    bool try_execute() {
        std::optional<T> val;
        bool closed_empty = false;
        if (channel->try_recv(val, closed_empty)) {
            if constexpr (kHandlesClose) {
                callback(std::move(val));
                return true;
            } else {
                if (!closed_empty && val.has_value()) {
                    callback(std::move(*val));
                    return true;
                }
                closed_skipped = true;
                return false;
            }
        }
        return false;
    }

    // Locked check during select() Phase 2 (channel lock is ALREADY held!)
    bool try_claim_locked(std::optional<detail::SelectWakeTarget>& wake_out) {
        bool closed_empty = false;
        if (channel->try_recv_locked(result_slot, closed_empty, wake_out)) {
            if constexpr (kHandlesClose) {
                return true;
            } else {
                if (!closed_empty && result_slot.has_value()) {
                    return true;
                }
                closed_skipped = true;
                return false;
            }
        }
        return false;
    }

    void enqueue_locked(detail::UniversalWaiter w, std::atomic<int>* winner,
                        detail::SpinLock* park_lock, int case_idx) noexcept {
        if (closed_skipped) return;
        waiter_node.waiter = w;
        waiter_node.data_slot = &result_slot;
        waiter_node.woken_by_close = false;
        waiter_node.select_winner = winner;
        waiter_node.select_park_lock = park_lock;
        waiter_node.select_case_index = case_idx;
        channel->enqueue_receiver_locked(&waiter_node);
    }

    void unlink() noexcept {
        channel->remove_receiver(&waiter_node);
    }

    void invoke_winner() {
        if constexpr (kHandlesClose) {
            if (waiter_node.woken_by_close) {
                callback(std::nullopt);
            } else {
                callback(std::move(result_slot));
            }
        } else {
            if (!waiter_node.woken_by_close && result_slot.has_value()) {
                callback(std::move(*result_slot));
            }
        }
    }
};

template <typename T, typename Callback>
struct SendCase {
    static constexpr bool kHandlesClose = std::is_invocable_v<Callback, bool>;

    Channel<T>* channel;
    T value;
    Callback callback;
    detail::ChannelWaiter<T> waiter_node{};
    bool closed_skipped{false};
    bool sent_ok{false};

    detail::SpinLock* lock_ptr() noexcept { return channel->internal_lock_ptr(); }

    bool try_execute() {
        bool is_closed = false;
        if (channel->try_send(value, is_closed)) {
            if constexpr (kHandlesClose) {
                callback(true);
            } else {
                callback();
            }
            return true;
        }
        if (is_closed) {
            if constexpr (kHandlesClose) {
                callback(false);
                return true;
            }
            closed_skipped = true;
        }
        return false;
    }

    bool try_claim_locked(std::optional<detail::SelectWakeTarget>& wake_out) {
        bool is_closed = false;
        if (channel->try_send_locked(value, is_closed, wake_out)) {
            sent_ok = true;
            return true;
        }
        if (is_closed) {
            if constexpr (kHandlesClose) {
                sent_ok = false;
                return true;
            }
            closed_skipped = true;
        }
        return false;
    }

    void enqueue_locked(detail::UniversalWaiter w, std::atomic<int>* winner,
                        detail::SpinLock* park_lock, int case_idx) noexcept {
        if (closed_skipped) return;
        waiter_node.waiter = w;
        waiter_node.data_slot = &value;
        waiter_node.woken_by_close = false;
        waiter_node.select_winner = winner;
        waiter_node.select_park_lock = park_lock;
        waiter_node.select_case_index = case_idx;
        channel->enqueue_sender_locked(&waiter_node);
    }

    void unlink() noexcept {
        channel->remove_sender(&waiter_node);
    }

    void invoke_winner() {
        if constexpr (kHandlesClose) {
            callback(sent_ok || !waiter_node.woken_by_close);
        } else {
            if (sent_ok || !waiter_node.woken_by_close) {
                callback();
            }
        }
    }
};

template <typename Callback>
struct DefaultCase {
    Callback callback;
};

template <typename Rep, typename Period, typename Callback>
struct TimeoutCase {
    std::chrono::duration<Rep, Period> duration;
    Callback callback;
};

template <typename T>
template <typename Callback>
auto Channel<T>::on_recv(Callback&& cb) {
    return RecvCase<T, std::decay_t<Callback>>{this, std::forward<Callback>(cb)};
}

template <typename T>
template <typename Callback>
auto Channel<T>::on_send(T val, Callback&& cb) {
    return SendCase<T, std::decay_t<Callback>>{this, std::move(val), std::forward<Callback>(cb)};
}

template <typename Callback>
auto on_default(Callback&& cb) {
    return DefaultCase<std::decay_t<Callback>>{std::forward<Callback>(cb)};
}

template <typename Rep, typename Period, typename Callback>
auto on_timeout(std::chrono::duration<Rep, Period> d, Callback&& cb) {
    return TimeoutCase<Rep, Period, std::decay_t<Callback>>{d, std::forward<Callback>(cb)};
}

namespace detail {

template <typename Case>
struct IsDefaultCase : std::false_type {};
template <typename Cb>
struct IsDefaultCase<DefaultCase<Cb>> : std::true_type {};

template <typename Case>
struct IsTimeoutCase : std::false_type {};
template <typename R, typename P, typename Cb>
struct IsTimeoutCase<TimeoutCase<R, P, Cb>> : std::true_type {};

inline std::uint64_t fast_select_rand() noexcept {
    thread_local std::uint64_t state = 0x9E3779B97F4A7C15ULL ^
        reinterpret_cast<std::uintptr_t>(&state);
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    return state;
}

} // namespace detail

/**
 * @brief Zero-busy-spin multi-channel `select` statement modeled on Go's `select { ... }` (`runtime.selectgo`).
 *
 * 1. **Phase 1 (Fast Poll):** Polls all channel clauses in pseudo-random order. If any clause is ready, executes it immediately.
 *    If none is ready and `on_default(...)` is present, executes the default clause immediately.
 * 2. **Phase 2 (True Parked Wait — 0% CPU):** Locks all participating channels in sorted pointer-address order
 *    (deadlock-free!), re-checks readiness, registers stack-allocated `ChannelWaiter` nodes on every channel and a
 *    cancellable timer for `on_timeout(...)`, and parks the routine cleanly via `park_current()`.
 *    Upon wakeup, immediately cancels the timer and unlinks all losing waiters before invoking the winning callback.
 */
template <typename... Cases>
void select(Cases&&... cases) {
    auto tuple_cases = std::make_tuple(std::forward<Cases>(cases)...);
    constexpr std::size_t N = sizeof...(Cases);
    if constexpr (N == 0) {
        return;
    }

    // Build pseudo-random poll order (Fisher-Yates with fast xorshift64)
    std::array<std::size_t, N> order{};
    for (std::size_t i = 0; i < N; ++i) order[i] = i;
    for (std::size_t i = N - 1; i > 0; --i) {
        std::size_t j = static_cast<std::size_t>(detail::fast_select_rand() % (i + 1));
        std::swap(order[i], order[j]);
    }

    // ---- PHASE 1: Fast Non-Blocking Poll ----
    for (std::size_t idx : order) {
        bool executed = false;
        auto try_one = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            (([&] {
                if (!executed && idx == Is) {
                    auto& c = std::get<Is>(tuple_cases);
                    using CType = std::decay_t<decltype(c)>;
                    if constexpr (!detail::IsDefaultCase<CType>::value &&
                                  !detail::IsTimeoutCase<CType>::value) {
                        if (c.try_execute()) {
                            executed = true;
                        }
                    }
                }
            }()), ...);
        };
        try_one(std::make_index_sequence<N>{});
        if (executed) return;
    }

    // Check DefaultCase or zero/negative TimeoutCase
    {
        bool finished = false;
        auto check_immediate = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            (([&] {
                if (finished) return;
                auto& c = std::get<Is>(tuple_cases);
                using CType = std::decay_t<decltype(c)>;
                if constexpr (detail::IsDefaultCase<CType>::value) {
                    c.callback();
                    finished = true;
                } else if constexpr (detail::IsTimeoutCase<CType>::value) {
                    if (c.duration <= decltype(c.duration)::zero()) {
                        c.callback();
                        finished = true;
                    }
                }
            }()), ...);
        };
        check_immediate(std::make_index_sequence<N>{});
        if (finished) return;
    }

    // ---- PHASE 2: True Parked Multi-Channel Wait (Zero Busy-Spin!) ----
    // 2.1 Collect and sort unique channel SpinLock* addresses to guarantee deadlock-free lock ordering
    std::array<detail::SpinLock*, N> lock_ptrs{};
    std::size_t lock_count = 0;
    {
        auto collect_locks = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            (([&] {
                auto& c = std::get<Is>(tuple_cases);
                using CType = std::decay_t<decltype(c)>;
                if constexpr (!detail::IsDefaultCase<CType>::value &&
                              !detail::IsTimeoutCase<CType>::value) {
                    if (!c.closed_skipped) {
                        lock_ptrs[lock_count++] = c.lock_ptr();
                    }
                }
            }()), ...);
        };
        collect_locks(std::make_index_sequence<N>{});
    }
    // Insertion sort + deduplicate SpinLock* pointers (avoids std::sort introsort array-bounds warnings on small N)
    for (std::size_t i = 1; i < lock_count; ++i) {
        detail::SpinLock* key = lock_ptrs[i];
        std::size_t j = i;
        while (j > 0 && std::less<detail::SpinLock*>{}(key, lock_ptrs[j - 1])) {
            lock_ptrs[j] = lock_ptrs[j - 1];
            --j;
        }
        lock_ptrs[j] = key;
    }
    if (lock_count > 1) {
        std::size_t write_idx = 1;
        for (std::size_t read_idx = 1; read_idx < lock_count; ++read_idx) {
            if (lock_ptrs[read_idx] != lock_ptrs[write_idx - 1]) {
                lock_ptrs[write_idx++] = lock_ptrs[read_idx];
            }
        }
        lock_count = write_idx;
    }

    auto lock_all = [&]() noexcept {
        for (std::size_t i = 0; i < lock_count; ++i) lock_ptrs[i]->lock();
    };
    auto unlock_all = [&]() noexcept {
        for (std::size_t i = lock_count; i > 0; --i) lock_ptrs[i - 1]->unlock();
    };

    std::atomic<int> winner{-1};
    detail::SpinLock park_lock;
    std::binary_semaphore fallback_sem{0};
    auto waiter = detail::UniversalWaiter::make_for_current(fallback_sem);

    // Track timer for TimeoutCase (if any)
    Runtime* timer_rt = (detail::current_worker() != nullptr)
                            ? &detail::current_worker()->runtime()
                            : &default_runtime();
    std::array<std::uint64_t, N> timer_ids{};
    std::size_t timer_count = 0;
    bool has_any_waiter = false;

    int immediate_win_idx = -1;
    std::optional<detail::SelectWakeTarget> immediate_wake;

    // Acquire park_lock BEFORE unlocking channel locks so no concurrent unpark can race with context_switch!
    park_lock.lock();
    lock_all();

    // 2.2 Re-check readiness under all channel locks in randomized order
    for (std::size_t idx : order) {
        auto check_locked = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            (([&] {
                if (immediate_win_idx != -1 || idx != Is) return;
                auto& c = std::get<Is>(tuple_cases);
                using CType = std::decay_t<decltype(c)>;
                if constexpr (!detail::IsDefaultCase<CType>::value &&
                              !detail::IsTimeoutCase<CType>::value) {
                    if (!c.closed_skipped && c.try_claim_locked(immediate_wake)) {
                        immediate_win_idx = static_cast<int>(Is);
                    }
                }
            }()), ...);
        };
        check_locked(std::make_index_sequence<N>{});
        if (immediate_win_idx != -1) break;
    }

    if (immediate_win_idx != -1) {
        unlock_all();
        park_lock.unlock();
        if (immediate_wake) {
            immediate_wake->wake();
        }
        auto invoke_imm = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            (([&] {
                if (immediate_win_idx == static_cast<int>(Is)) {
                    auto& c = std::get<Is>(tuple_cases);
                    using CType = std::decay_t<decltype(c)>;
                    if constexpr (!detail::IsDefaultCase<CType>::value &&
                                  !detail::IsTimeoutCase<CType>::value) {
                        c.invoke_winner();
                    }
                }
            }()), ...);
        };
        invoke_imm(std::make_index_sequence<N>{});
        return;
    }

    // 2.3 Enqueue stack-allocated waiters into all open channels while holding all channel locks
    {
        auto enqueue_all = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            (([&] {
                auto& c = std::get<Is>(tuple_cases);
                using CType = std::decay_t<decltype(c)>;
                if constexpr (!detail::IsDefaultCase<CType>::value &&
                              !detail::IsTimeoutCase<CType>::value) {
                    if (!c.closed_skipped) {
                        c.enqueue_locked(waiter, &winner, &park_lock, static_cast<int>(Is));
                        has_any_waiter = true;
                    }
                }
            }()), ...);
        };
        enqueue_all(std::make_index_sequence<N>{});
    }

    unlock_all();

    // 2.4 Register cancellable timer(s) for TimeoutCase(s)
    {
        auto register_timers = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            (([&] {
                auto& c = std::get<Is>(tuple_cases);
                using CType = std::decay_t<decltype(c)>;
                if constexpr (detail::IsTimeoutCase<CType>::value) {
                    has_any_waiter = true;
                    auto deadline = std::chrono::steady_clock::now() +
                                    std::chrono::duration_cast<std::chrono::steady_clock::duration>(c.duration);
                    int case_idx = static_cast<int>(Is);
                    std::uint64_t tid = timer_rt->schedule_timer_callback(
                        deadline,
                        [&winner, &park_lock, waiter, case_idx]() mutable {
                            int expected = -1;
                            if (winner.compare_exchange_strong(expected, case_idx, std::memory_order_acq_rel)) {
                                park_lock.lock();
                                park_lock.unlock();
                                waiter.unpark();
                            }
                        }
                    );
                    timer_ids[timer_count++] = tid;
                }
            }()), ...);
        };
        register_timers(std::make_index_sequence<N>{});
    }

    if (!has_any_waiter) {
        // All channels were already closed and no timeout/default clause exists — avoid deadlocking forever
        park_lock.unlock();
        return;
    }

    // 2.5 Atomically park until one channel or timer wins `winner`!
    waiter.park_current(park_lock);

    // ---- WOKEN UP ----
    // 2.6 Immediately cancel any pending timeout timers and unlink all losing waiters from every channel!
    for (std::size_t i = 0; i < timer_count; ++i) {
        timer_rt->cancel_timer(timer_ids[i]);
    }
    {
        auto unlink_all = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            (([&] {
                auto& c = std::get<Is>(tuple_cases);
                using CType = std::decay_t<decltype(c)>;
                if constexpr (!detail::IsDefaultCase<CType>::value &&
                              !detail::IsTimeoutCase<CType>::value) {
                    if (!c.closed_skipped) {
                        c.unlink();
                    }
                }
            }()), ...);
        };
        unlink_all(std::make_index_sequence<N>{});
    }

    // 2.7 Invoke the winning case's callback!
    int won = winner.load(std::memory_order_acquire);
    {
        auto dispatch_winner = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            (([&] {
                if (won == static_cast<int>(Is)) {
                    auto& c = std::get<Is>(tuple_cases);
                    using CType = std::decay_t<decltype(c)>;
                    if constexpr (detail::IsTimeoutCase<CType>::value) {
                        c.callback();
                    } else if constexpr (!detail::IsDefaultCase<CType>::value) {
                        c.invoke_winner();
                    }
                }
            }()), ...);
        };
        dispatch_winner(std::make_index_sequence<N>{});
    }
}

} // namespace gorun
// ---- end gorun/channel.hpp ----
// ---- begin gorun/context_go.hpp ----

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
// ---- end gorun/context_go.hpp ----
// ---- begin gorun/task_group.hpp ----

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
// ---- end gorun/task_group.hpp ----
// ---- end gorun/gorun.hpp ----
