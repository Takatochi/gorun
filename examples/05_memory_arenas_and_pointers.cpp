/**
 * @file 05_memory_arenas_and_pointers.cpp
 * @brief Example 5: Large-Scale Memory Management, Custom Bump Arena Allocator,
 *        Zero-Copy `std::unique_ptr` Slab Pipelines, `std::shared_ptr` Graph Broadcast,
 *        and Stack-Pointer Stability Verification across M:N Worker Migrations.
 *
 * Demonstrates 4 advanced C++20 memory and pointer architectures on top of `gorun`:
 *  1. Per-Request Bump Arena Allocator (`RequestArena` + `GORUN_DEFER { arena.reset(); }`)
 *     for zero-fragmentation HTTP/RPC packet processing.
 *  2. Zero-Copy Exclusive Ownership Transfer (`std::unique_ptr<PacketSlab>`) across a 3-stage
 *     MPMC channel pipeline (allocated once, mutated in-place across cores, freed once).
 *  3. Shared Immutable Snapshot Broadcast (`std::shared_ptr<const RoutingTable>`) read concurrently
 *     by 32 green routines without copying or locking.
 *  4. Direct Stack-Pointer Tree (`Node*` allocated on a parent routine's stack frame and mutated
 *     in parallel by child routines inside a `gorun::Scope`, leveraging the Stack Stability Invariant).
 */

#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <new>
#include <string_view>
#include <vector>

#include "gorun/gorun.hpp"

// ============================================================================
// 1. Custom Linear Bump Arena Allocator (Zero-Fragmentation Request Memory)
// ============================================================================
class LinearArena {
    std::unique_ptr<std::byte[]> memory_;
    std::size_t capacity_;
    std::size_t offset_{0};
    std::size_t allocations_{0};

public:
    explicit LinearArena(std::size_t capacity_bytes)
        : memory_(std::make_unique<std::byte[]>(capacity_bytes))
        , capacity_(capacity_bytes) {}

    template <typename T, typename... Args>
    T* create(Args&&... args) {
        constexpr std::size_t align = alignof(T);
        std::size_t aligned_offset = (offset_ + align - 1) & ~(align - 1);
        if (aligned_offset + sizeof(T) > capacity_) {
            throw std::bad_alloc();
        }
        void* ptr = memory_.get() + aligned_offset;
        offset_ = aligned_offset + sizeof(T);
        ++allocations_;
        return ::new (ptr) T(std::forward<Args>(args)...);
    }

    void reset() noexcept {
        offset_ = 0;
        allocations_ = 0;
    }

    [[nodiscard]] std::size_t bytes_used() const noexcept { return offset_; }
    [[nodiscard]] std::size_t allocation_count() const noexcept { return allocations_; }
};

// ============================================================================
// 2. Zero-Copy Network Packet Slab (Move-Only via std::unique_ptr)
// ============================================================================
struct PacketSlab {
    static inline std::atomic<int> active_slabs{0};

    std::uint64_t packet_id{0};
    std::array<std::uint32_t, 64> payload{};
    std::uint64_t checksum{0};
    const PacketSlab* original_heap_address{nullptr};

    explicit PacketSlab(std::uint64_t id) noexcept : packet_id(id) {
        active_slabs.fetch_add(1, std::memory_order_relaxed);
        original_heap_address = this;
        for (std::size_t i = 0; i < payload.size(); ++i) {
            payload[i] = static_cast<std::uint32_t>(id + i);
        }
    }

    PacketSlab(const PacketSlab&) = delete;
    PacketSlab& operator=(const PacketSlab&) = delete;

    ~PacketSlab() noexcept {
        active_slabs.fetch_sub(1, std::memory_order_relaxed);
    }
};

// ============================================================================
// 3. Stack-Allocated Tree Node (Demonstrating Stack Pointer Stability)
// ============================================================================
struct StackWorkNode {
    int node_id{0};
    int computed_result{0};
    const StackWorkNode* self_stack_addr{nullptr};
};

int main() {
    std::cout << "=== Example 05: Advanced Memory Arenas, Zero-Copy Pointers & Stack Stability ===\n";

    // ------------------------------------------------------------------------
    // PART A: Zero-Copy std::unique_ptr<PacketSlab> 3-Stage Pipeline
    // Prove that moving unique_ptr through channels never copies the underlying
    // PacketSlab in memory (`slab->original_heap_address == slab.get()`) and
    // destroys 100% of slabs upon completion (`PacketSlab::active_slabs == 0`).
    // ------------------------------------------------------------------------
    {
        gorun::Channel<std::unique_ptr<PacketSlab>> ingest_ch(16);
        gorun::Channel<std::unique_ptr<PacketSlab>> verify_ch(0); // Unbuffered stack-to-stack handoff!
        std::atomic<std::uint64_t> total_verified_packets{0};

        gorun::Scope pipeline_scope;

        // Stage 1: Allocator / Ingest Producer
        gorun::go([&]() {
            GORUN_DEFER { ingest_ch.close(); };
            for (std::uint64_t id = 1; id <= 200; ++id) {
                auto slab = std::make_unique<PacketSlab>(id);
                ingest_ch.send(std::move(slab));
            }
        });

        // Stage 2: 4 Parallel Checksum Workers (mutate PacketSlab in-place without reallocation)
        gorun::WaitGroup workers_wg;
        workers_wg.add(4);
        for (int w = 0; w < 4; ++w) {
            gorun::go([&]() {
                GORUN_DEFER { workers_wg.done(); };
                while (auto slab_opt = ingest_ch.recv()) {
                    std::unique_ptr<PacketSlab> slab = std::move(*slab_opt);
                    // Verify zero-copy: heap pointer address never changed!
                    assert(slab.get() == slab->original_heap_address);

                    std::uint64_t sum = 0;
                    for (std::uint32_t v : slab->payload) {
                        sum += v;
                    }
                    slab->checksum = sum;
                    verify_ch.send(std::move(slab));
                }
            });
        }

        // Coordinator closes verify_ch once all 4 workers finish
        gorun::go([&]() {
            workers_wg.wait();
            verify_ch.close();
        });

        // Stage 3: Sink Consumer (verifies checksum and lets unique_ptr destroy the slab)
        gorun::go([&]() {
            while (auto slab_opt = verify_ch.recv()) {
                std::unique_ptr<PacketSlab> slab = std::move(*slab_opt);
                assert(slab.get() == slab->original_heap_address);
                assert(slab->checksum > 0);
                total_verified_packets.fetch_add(1, std::memory_order_relaxed);
            }
        });

        pipeline_scope.wait();
        std::cout << "[Part A: Zero-Copy unique_ptr Pipeline] Processed " << total_verified_packets.load()
                  << " slabs | Active leaks remaining: " << PacketSlab::active_slabs.load() << "\n";
        assert(total_verified_packets.load() == 200);
        assert(PacketSlab::active_slabs.load() == 0);
    }

    // ------------------------------------------------------------------------
    // PART B: Per-Routine Linear Bump Arena with GORUN_DEFER Reset
    // ------------------------------------------------------------------------
    {
        struct HttpHeaderNode {
            std::uint64_t hash;
            const char* key;
            const char* value;
            HttpHeaderNode* next;
        };

        gorun::Scope arena_scope;
        std::atomic<std::size_t> total_arena_allocs{0};

        for (int req = 0; req < 16; ++req) {
            gorun::go([&, req]() {
                LinearArena arena(16 * 1024); // 16 KB arena per green routine
                GORUN_DEFER {
                    // Instant O(1) bulk deallocation of all arena pointers at routine exit!
                    arena.reset();
                };

                HttpHeaderNode* head = nullptr;
                for (int h = 0; h < 50; ++h) {
                    head = arena.create<HttpHeaderNode>(
                        static_cast<std::uint64_t>(req * 100 + h),
                        "X-Gorun-Header",
                        "Zero-Overhead",
                        head
                    );
                }

                // Yield mid-request to force routine migration across OS worker threads
                gorun::yield();

                // Traverse raw pointers inside the arena after migration
                std::size_t count = 0;
                for (HttpHeaderNode* cur = head; cur != nullptr; cur = cur->next) {
                    if (cur->hash < static_cast<std::uint64_t>(req * 100)) std::abort();
                    ++count;
                }
                if (count != 50) std::abort();
                total_arena_allocs.fetch_add(arena.allocation_count(), std::memory_order_relaxed);
            });
        }
        arena_scope.wait();
        std::cout << "[Part B: Linear Bump Arenas] Completed " << total_arena_allocs.load()
                  << " zero-fragmentation arena allocations across 16 migrating routines.\n";
    }

    // ------------------------------------------------------------------------
    // PART C: Direct Stack-Pointer Tree (Parent Stack -> 32 Child Routines)
    // Because of gorun's Stack Stability Invariant AND Structured Scope, a parent
    // green routine can allocate an array of nodes on its OWN stack frame and hand
    // raw pointers (`StackWorkNode*`) to 32 child routines!
    // ------------------------------------------------------------------------
    {
        gorun::Scope outer_scope;

        gorun::go([]() {
            // Allocated directly on this parent green routine's 128 KB stack!
            std::array<StackWorkNode, 32> stack_nodes{};
            for (int i = 0; i < 32; ++i) {
                stack_nodes[i].node_id = i + 1;
                stack_nodes[i].self_stack_addr = &stack_nodes[i];
            }

            {
                gorun::Scope inner_scope;
                for (int i = 0; i < 32; ++i) {
                    // Pass a raw pointer into the parent routine's stack frame to a child routine!
                    StackWorkNode* raw_stack_ptr = &stack_nodes[i];
                    gorun::go([raw_stack_ptr]() {
                        for (int step = 0; step < 10; ++step) {
                            gorun::yield();
                            // Parent stack never moves even while parent is parked on ~inner_scope()!
                            assert(raw_stack_ptr == raw_stack_ptr->self_stack_addr);
                            raw_stack_ptr->computed_result += raw_stack_ptr->node_id;
                        }
                    });
                }
            } // <-- ~inner_scope() parks the parent routine until all 32 children finish mutating `stack_nodes`!

            int total_stack_sum = 0;
            for (const auto& node : stack_nodes) {
                assert(&node == node.self_stack_addr);
                assert(node.computed_result == node.node_id * 10);
                total_stack_sum += node.computed_result;
            }
            std::cout << "[Part C: Cross-Routine Raw Stack Pointers] Verified 32 child routines mutating parent stack frame: sum = "
                      << total_stack_sum << "\n";
        });
    }

    return 0;
}
