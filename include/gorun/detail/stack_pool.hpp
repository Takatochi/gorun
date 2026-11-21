#pragma once

/**
 * @file stack_pool.hpp
 * @brief Multi-bucket virtual-memory stack allocator and recycling pool (`StackPool`) with
 *        hardware Guard Pages, configurable per-routine stack sizes (`StackSize`, `Options`),
 *        and observability counters (`StackPoolStats`).
 */

#include <algorithm>
#include <array>
#include <atomic>
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

namespace gorun {

/**
 * @enum StackSize
 * @brief Predefined stack size classes for launching green routines with `gorun::go(StackSize::Small, fn)`.
 */
enum class StackSize : std::size_t {
    Small   = 16 * 1024,       ///< 16 KB  — lightweight tasks & high-density fan-out
    Compact = 64 * 1024,       ///< 64 KB  — medium tasks & simple network handlers
    Default = 128 * 1024,      ///< 128 KB — general-purpose handlers & I/O
    Medium  = 256 * 1024,      ///< 256 KB — deeper call stacks / parsing
    Large   = 512 * 1024,      ///< 512 KB — heavy recursion / large stack buffers
    Huge    = 2 * 1024 * 1024  ///< 2 MB   — deep recursion or C library interop
};

/**
 * @struct Options
 * @brief Per-routine spawn configuration passed to `gorun::go(Options{.stack_size = ...}, fn)`.
 */
struct Options {
    std::size_t stack_size{0}; ///< Requested usable stack size in bytes (`0` = Runtime's `default_stack_size`)
};

/**
 * @struct StackPoolStats
 * @brief Snapshot of virtual-memory stack pool allocations and cache reuse.
 */
struct StackPoolStats {
    std::size_t pooled_stacks{0};           ///< Idle stacks currently cached across all size buckets
    std::size_t pooled_bytes{0};            ///< Total usable stack bytes currently cached in pool
    std::uint64_t total_allocated{0};       ///< Total OS `VirtualAlloc`/`mmap` stack allocations
    std::uint64_t total_reused{0};          ///< Total pool cache hits (avoided OS syscalls)
};

namespace detail {

/**
 * @struct StackAllocation
 * @brief Descriptor for an OS-allocated green routine stack with an inaccessible hardware Guard Page at the bottom.
 *
 * Guarantees the **Stack Stability** invariant: fixed virtual-memory stacks never move during a routine's
 * lifetime, so pointers/references to stack variables remain 100% valid across suspension points.
 */
struct StackAllocation {
    void* base_allocation{nullptr}; ///< Base address returned by `VirtualAlloc` / `mmap` (starts with the Guard Page)
    std::size_t total_bytes{0};     ///< Total virtual memory region size in bytes (usable stack + guard page)
    void* stack_bottom{nullptr};    ///< Lowest usable stack address (immediately above the guard page)
    void* stack_top{nullptr};       ///< Highest usable stack address (initial `SP`; stack grows downward)
    std::size_t usable_bytes{0};    ///< Usable stack capacity in bytes
};

/**
 * @class StackPool
 * @brief Thread-safe multi-bucket recycling pool of routine stacks (`StackAllocation`).
 *
 * Maintains 8 power-of-two size-class buckets (16 KB, 32 KB, 64 KB, 128 KB, 256 KB, 512 KB, 1 MB, 2 MB)
 * so applications mixing lightweight 16 KB routines and deep 512 KB routines both enjoy zero-syscall LIFO reuse.
 */
class StackPool {
    static constexpr std::size_t kMinStackBytes = 16 * 1024;
    static constexpr std::size_t kBucketCount = 8;
    static constexpr std::array<std::size_t, kBucketCount> kBucketSizes = {
        16 * 1024,
        32 * 1024,
        64 * 1024,
        128 * 1024,
        256 * 1024,
        512 * 1024,
        1024 * 1024,
        2048 * 1024
    };

    std::size_t default_usable_size_;
    std::size_t max_pooled_;
    mutable std::mutex mutex_;
    std::array<std::vector<StackAllocation>, kBucketCount> buckets_{};
    std::size_t pooled_total_{0};
    std::size_t pooled_bytes_{0};
    std::atomic<std::uint64_t> total_allocated_{0};
    std::atomic<std::uint64_t> total_reused_{0};

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

    /// Maps a usable size to its bucket index, or `kBucketCount` if larger than 2 MB.
    static std::size_t bucket_index_for(std::size_t usable_bytes) noexcept {
        for (std::size_t i = 0; i < kBucketCount; ++i) {
            if (usable_bytes <= kBucketSizes[i]) {
                return i;
            }
        }
        return kBucketCount;
    }

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
    explicit StackPool(std::size_t default_usable_size = 128 * 1024, std::size_t max_pooled = 1024)
        : default_usable_size_(normalize_size(default_usable_size))
        , max_pooled_(max_pooled) {}

    ~StackPool() {
        std::lock_guard lock(mutex_);
        for (auto& bucket : buckets_) {
            for (const auto& s : bucket) {
                os_deallocate(s);
            }
            bucket.clear();
        }
        pooled_total_ = 0;
        pooled_bytes_ = 0;
    }

    /// Normalizes a requested stack size into its rounded bucket size (or page-aligned custom size if > 2 MB).
    [[nodiscard]] static std::size_t normalize_size(std::size_t requested) noexcept {
        const std::size_t clamped = std::max(requested, kMinStackBytes);
        const std::size_t idx = bucket_index_for(clamped);
        if (idx < kBucketCount) {
            return kBucketSizes[idx];
        }
        return align_up(clamped, page_size());
    }

    [[nodiscard]] std::size_t default_usable_size() const noexcept {
        return default_usable_size_;
    }

    /**
     * @brief Acquires a warm stack matching `requested_bytes` (0 = default size) from the matching bucket,
     *        or allocates a fresh guard-paged virtual-memory stack from the OS.
     */
    StackAllocation acquire(std::size_t requested_bytes = 0) {
        const std::size_t target = (requested_bytes == 0)
            ? default_usable_size_
            : normalize_size(requested_bytes);
        const std::size_t idx = bucket_index_for(target);

        if (idx < kBucketCount) {
            std::lock_guard lock(mutex_);
            auto& bucket = buckets_[idx];
            if (!bucket.empty()) {
                StackAllocation s = bucket.back();
                bucket.pop_back();
                --pooled_total_;
                pooled_bytes_ -= s.usable_bytes;
                total_reused_.fetch_add(1, std::memory_order_relaxed);
                return s;
            }
        }

        StackAllocation alloc = os_allocate(target);
        total_allocated_.fetch_add(1, std::memory_order_relaxed);
        return alloc;
    }

    /**
     * @brief Returns a finished routine's stack to its size-class bucket (or releases to OS if full/oversized).
     */
    void release(StackAllocation s) noexcept {
        if (!s.base_allocation) return;
        const std::size_t idx = bucket_index_for(s.usable_bytes);
        if (idx < kBucketCount && s.usable_bytes == kBucketSizes[idx]) {
            std::lock_guard lock(mutex_);
            if (pooled_total_ < max_pooled_) {
                buckets_[idx].push_back(s);
                ++pooled_total_;
                pooled_bytes_ += s.usable_bytes;
                return;
            }
        }
        os_deallocate(s);
    }

    [[nodiscard]] std::size_t pooled_count() const noexcept {
        std::lock_guard lock(mutex_);
        return pooled_total_;
    }

    [[nodiscard]] StackPoolStats stats() const noexcept {
        std::lock_guard lock(mutex_);
        return StackPoolStats{
            .pooled_stacks = pooled_total_,
            .pooled_bytes = pooled_bytes_,
            .total_allocated = total_allocated_.load(std::memory_order_relaxed),
            .total_reused = total_reused_.load(std::memory_order_relaxed)
        };
    }
};

} // namespace detail
} // namespace gorun
