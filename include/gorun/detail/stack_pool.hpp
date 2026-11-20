#pragma once

/**
 * @file stack_pool.hpp
 * @brief Virtual memory stack allocator and recycling pool (`StackPool`) with hardware Guard Pages.
 */

#include <cstddef>
#include <cstdint>
#include <cstdlib>
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
        return static_cast<std::size_t>(si.dwPageSize);
#else
        return static_cast<std::size_t>(::sysconf(_SC_PAGESIZE));
#endif
    }

    static std::size_t align_up(std::size_t size, std::size_t alignment) noexcept {
        return (size + alignment - 1) & ~(alignment - 1);
    }

    /**
     * @brief Allocates a fresh stack from OS virtual memory and marks the lowest page as `PAGE_NOACCESS` / `PROT_NONE`.
     * @param requested_usable Requested usable stack size in bytes.
     * @throws std::bad_alloc If OS virtual memory allocation fails.
     */
    static StackAllocation os_allocate(std::size_t requested_usable) {
        const std::size_t pg = page_size();
        const std::size_t usable = align_up(requested_usable, pg);
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
     * @param default_usable_size Default usable stack size per routine in bytes (default: 128 KB).
     * @param max_pooled Maximum number of idle stacks retained in the pool before returning memory to the OS.
     */
    explicit StackPool(std::size_t default_usable_size = 128 * 1024, std::size_t max_pooled = 1024)
        : default_usable_size_(default_usable_size)
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
