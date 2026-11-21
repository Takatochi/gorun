#pragma once

/**
 * @file context.hpp
 * @brief Ultra-fast user-space CPU execution context (`ExecutionContext`) and assembly stack switching
 *        for x86_64 (Windows x64 ABI & System V AMD64 ABI) and ARM64 / AArch64 (AAPCS64: Linux ARM64,
 *        Apple Silicon macOS M1-M4, Windows ARM64).
 *
 * Supports cross-thread migration of green routines across M:N OS worker threads (Work-Stealing)
 * with full callee-saved GPR, SIMD/FP, FPU control word / FPCR, and Windows `NT_TIB` stack-bound preservation,
 * plus ASan/TSan no-sanitize annotations on the raw register swap boundary.
 */

#include <atomic>
#include <cstddef>
#include <cstdint>
#include "gorun/detail/stack_pool.hpp"

// Compiler-portable sanitizer suppression macros for raw stack-switching boundaries
#if defined(__clang__)
#define GORUN_NO_SANITIZE_FIBER __attribute__((no_sanitize("address", "thread")))
#elif defined(__GNUC__)
#define GORUN_NO_SANITIZE_FIBER __attribute__((no_sanitize_address))
#else
#define GORUN_NO_SANITIZE_FIBER
#endif

// Apple Mach-O prefixes C external symbols with a leading underscore
#if defined(__APPLE__)
#define GORUN_ASM_SYM(name) "_" #name
#define GORUN_ASM_TYPE_FUNC(name)
#else
#define GORUN_ASM_SYM(name) #name
#if defined(_WIN32)
#define GORUN_ASM_TYPE_FUNC(name)
#else
#define GORUN_ASM_TYPE_FUNC(name) ".type " #name ", @function\n"
#endif
#endif

namespace gorun::detail {

extern "C" void gorun_context_switch_asm(void** from_rsp, void** to_rsp) noexcept;
extern "C" void gorun_routine_entry_asm() noexcept;

#if defined(_MSC_VER) && !defined(__clang__)
#error "gorun requires a GNU/Clang inline-assembly compatible compiler (GCC, Clang, or MinGW-w64)."

// ============================================================================
// Architecture 1: x86_64 (Windows x64 ABI + System V AMD64 ABI on Linux/macOS)
// ============================================================================
#elif defined(__x86_64__) || defined(_M_X64)

inline constexpr const char* kArchitectureName = "x86_64";

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
    ".globl " GORUN_ASM_SYM(gorun_context_switch_asm) "\n"
    GORUN_ASM_TYPE_FUNC(gorun_context_switch_asm)
    GORUN_ASM_SYM(gorun_context_switch_asm) ":\n"
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
    ".globl " GORUN_ASM_SYM(gorun_routine_entry_asm) "\n"
    GORUN_ASM_TYPE_FUNC(gorun_routine_entry_asm)
    GORUN_ASM_SYM(gorun_routine_entry_asm) ":\n"
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
 */
struct ExecutionContext {
    void* rsp{nullptr}; ///< Saved stack pointer (`%rsp`)

    void init_scheduler_for_current_thread() noexcept {}
    void cleanup_scheduler_for_current_thread() noexcept {}
    void destroy_routine_context() noexcept {}

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

// ============================================================================
// Architecture 2: ARM64 / AArch64 (AAPCS64: Apple Silicon M1-M4, AWS Graviton, Linux ARM64)
// ============================================================================
#elif defined(__aarch64__) || defined(_M_ARM64)

inline constexpr const char* kArchitectureName = "aarch64";

// AAPCS64 callee-saved frame layout (176 bytes, 16-byte aligned):
//   [sp +   0]: x19, x20   (x19 = arg, x20 = trampoline on initial entry)
//   [sp +  16]: x21, x22
//   [sp +  32]: x23, x24
//   [sp +  48]: x25, x26
//   [sp +  64]: x27, x28
//   [sp +  80]: x29 (FP), x30 (LR)
//   [sp +  96]: d8,  d9
//   [sp + 112]: d10, d11
//   [sp + 128]: d12, d13
//   [sp + 144]: d14, d15
//   [sp + 160]: fpcr, fpsr
__asm__(
    ".text\n"
    ".p2align 2\n"
    ".globl " GORUN_ASM_SYM(gorun_context_switch_asm) "\n"
    GORUN_ASM_TYPE_FUNC(gorun_context_switch_asm)
    GORUN_ASM_SYM(gorun_context_switch_asm) ":\n"
    "  .cfi_startproc\n"
    "  sub sp, sp, #176\n"
    "  stp x19, x20, [sp, #0]\n"
    "  stp x21, x22, [sp, #16]\n"
    "  stp x23, x24, [sp, #32]\n"
    "  stp x25, x26, [sp, #48]\n"
    "  stp x27, x28, [sp, #64]\n"
    "  stp x29, x30, [sp, #80]\n"
    "  stp d8,  d9,  [sp, #96]\n"
    "  stp d10, d11, [sp, #112]\n"
    "  stp d12, d13, [sp, #128]\n"
    "  stp d14, d15, [sp, #144]\n"
    "  mrs x9,  fpcr\n"
    "  mrs x10, fpsr\n"
    "  stp x9,  x10, [sp, #160]\n"
    // Switch SP: x0 = from_rsp, x1 = to_rsp
    "  mov x9, sp\n"
    "  str x9, [x0]\n"
    "  ldr x9, [x1]\n"
    "  mov sp, x9\n"
    // Restore callee-saved FPCR/FPSR, SIMD d8-d15, and GPRs x19-x30
    "  ldp x9,  x10, [sp, #160]\n"
    "  msr fpcr, x9\n"
    "  msr fpsr, x10\n"
    "  ldp d14, d15, [sp, #144]\n"
    "  ldp d12, d13, [sp, #128]\n"
    "  ldp d10, d11, [sp, #112]\n"
    "  ldp d8,  d9,  [sp, #96]\n"
    "  ldp x29, x30, [sp, #80]\n"
    "  ldp x27, x28, [sp, #64]\n"
    "  ldp x25, x26, [sp, #48]\n"
    "  ldp x23, x24, [sp, #32]\n"
    "  ldp x21, x22, [sp, #16]\n"
    "  ldp x19, x20, [sp, #0]\n"
    "  add sp, sp, #176\n"
    "  ret\n"
    "  .cfi_endproc\n"
    ".p2align 2\n"
    ".globl " GORUN_ASM_SYM(gorun_routine_entry_asm) "\n"
    GORUN_ASM_TYPE_FUNC(gorun_routine_entry_asm)
    GORUN_ASM_SYM(gorun_routine_entry_asm) ":\n"
    "  .cfi_startproc\n"
    "  mov x0, x19\n"
    "  blr x20\n"
    "  brk #0\n"
    "  .cfi_endproc\n"
);

struct ExecutionContext {
    void* rsp{nullptr}; ///< Saved stack pointer (`sp`)

    void init_scheduler_for_current_thread() noexcept {}
    void cleanup_scheduler_for_current_thread() noexcept {}
    void destroy_routine_context() noexcept {}

    void make_routine_stack_context(const StackAllocation& stack, void (*trampoline)(void*), void* arg) noexcept {
        auto top = reinterpret_cast<std::uintptr_t>(stack.stack_top);
        top &= ~static_cast<std::uintptr_t>(15);

        // Allocate 176-byte (22 x uint64_t) AAPCS64 frame matching gorun_context_switch_asm
        auto* sp = reinterpret_cast<std::uint64_t*>(top) - 22;
        for (int i = 0; i < 22; ++i) {
            sp[i] = 0;
        }
        sp[0]  = reinterpret_cast<std::uint64_t>(arg);                      // x19 -> arg (x0 in entry)
        sp[1]  = reinterpret_cast<std::uint64_t>(trampoline);               // x20 -> trampoline
        sp[11] = reinterpret_cast<std::uint64_t>(&gorun_routine_entry_asm); // x30 (LR) -> return address for `ret`
        rsp = sp;
    }
};

#else
#error "gorun: unsupported CPU architecture (x86_64 and ARM64/AArch64 are supported)."
#endif

/**
 * @brief Performs an ultra-fast (~10-15 ns) user-space context switch between `from` and `to`.
 *
 * Marked `noinline` + no-sanitize with compiler signal fences so optimizing compilers (`-O2`/`-O3`)
 * never cache `thread_local` pointers (`current_worker()`, `current_routine()`) in registers across
 * a routine migration between different OS worker threads.
 */
#if defined(__GNUC__) || defined(__clang__)
__attribute__((noinline)) GORUN_NO_SANITIZE_FIBER
#elif defined(_MSC_VER)
__declspec(noinline)
#endif
inline void context_switch(ExecutionContext* from, ExecutionContext* to) noexcept {
    std::atomic_signal_fence(std::memory_order_seq_cst);
    gorun_context_switch_asm(&from->rsp, &to->rsp);
    std::atomic_signal_fence(std::memory_order_seq_cst);
}

} // namespace gorun::detail
