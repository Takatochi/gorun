#pragma once

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
#include "gorun/detail/stack_pool.hpp"

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
