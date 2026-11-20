/**
 * @file 04_worker_pool_and_sync.cpp
 * @brief Example 4: MPMC Worker Pool, `gorun::RwMutex` Read-Through Cache,
 *        `gorun::Once` Lazy Initialization, and `gorun::go_async` (`JoinHandle<T>`).
 */

#include <chrono>
#include <iostream>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "gorun/gorun.hpp"

using namespace std::chrono_literals;

class GreenThreadSafeCache {
    gorun::Once init_once_;
    gorun::RwMutex rw_mtx_;
    std::unordered_map<int, std::string> store_;

public:
    void ensure_initialized() {
        gorun::call_once(init_once_, [&]() {
            std::unique_lock<gorun::RwMutex> wlock(rw_mtx_);
            store_[1] = "Cached-Config-Alpha";
            store_[2] = "Cached-Config-Beta";
            std::cout << "[Once] Cache initialized exactly once!\n";
        });
    }

    std::string get_or_compute(int key) {
        ensure_initialized();

        // 1. Fast concurrent read path via std::shared_lock<gorun::RwMutex>
        {
            std::shared_lock<gorun::RwMutex> rlock(rw_mtx_);
            if (auto it = store_.find(key); it != store_.end()) {
                return it->second;
            }
        }

        // 2. Cache miss: upgrade to exclusive writer lock via std::unique_lock<gorun::RwMutex>
        std::unique_lock<gorun::RwMutex> wlock(rw_mtx_);
        auto [it, inserted] = store_.emplace(key, "Computed-Value-" + std::to_string(key * 100));
        return it->second;
    }
};

int main() {
    std::cout << "=== Example 04: Worker Pool, RwMutex Cache, Once & JoinHandle<T> ===\n";

    GreenThreadSafeCache cache;
    std::vector<gorun::JoinHandle<std::string>> futures;

    // Launch 8 asynchronous green routines returning JoinHandle<std::string>
    for (int i = 1; i <= 8; ++i) {
        int key = (i % 4) + 1;
        futures.push_back(gorun::go_async([&, key]() -> std::string {
            gorun::yield();
            return cache.get_or_compute(key);
        }));
    }

    for (std::size_t i = 0; i < futures.size(); ++i) {
        std::cout << "[JoinHandle #" << (i + 1) << "] Result: " << futures[i].get() << "\n";
    }

    return 0;
}
