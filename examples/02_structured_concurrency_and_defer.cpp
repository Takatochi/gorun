/**
 * @file 02_structured_concurrency_and_defer.cpp
 * @brief Example 2: Structured Concurrency (`gorun::Scope`, `gorun::TaskGroup`),
 *        Safe `[&]` Reference Captures, Cooperative Cancellation (`std::stop_token`),
 *        and Transaction Rollbacks with `gorun::defer(...).dismiss()`.
 */

#include <chrono>
#include <iostream>
#include <numeric>
#include <stdexcept>
#include <vector>

#include "gorun/gorun.hpp"

using namespace std::chrono_literals;

// Demonstrates transaction commit/rollback using gorun::defer(...).dismiss()
void execute_payment_transaction(int tx_id, bool simulate_failure) {
    std::cout << "[Tx #" << tx_id << "] Opening ledger transaction...\n";

    // Armed on the routine's stack: runs automatically on return or exception unless dismissed!
    auto rollback = gorun::defer([tx_id]() {
        std::cout << "[Tx #" << tx_id << "] ROLLBACK executed automatically!\n";
    });

    gorun::sleep_for(5ms);
    if (simulate_failure) {
        throw std::runtime_error("Insufficient funds in account");
    }

    std::cout << "[Tx #" << tx_id << "] COMMIT succeeded!\n";
    rollback.dismiss(); // Disarms the rollback guard after commit
}

int main() {
    std::cout << "=== Example 02: Structured Concurrency (Scope) & Defer Rollbacks ===\n";

    // 1. Safe [&] reference capture of local stack vector inside gorun::Scope
    std::vector<int> partial_sums(8, 0);
    {
        gorun::Scope scope; // ~Scope() guarantees all child go(...) calls finish before exiting this block!

        for (int i = 0; i < 8; ++i) {
            gorun::go([&, i]() {
                gorun::yield();
                partial_sums[i] = (i + 1) * 10;
            });
        }
    } // <-- All 8 green routines are 100% finished here.

    int total = std::accumulate(partial_sums.begin(), partial_sums.end(), 0);
    std::cout << "[Scope] Parallel sum of 8 stack-captured slices: " << total << "\n";

    // 2. Defer rollback guard in action (Success vs. Exception)
    execute_payment_transaction(101, false);
    try {
        execute_payment_transaction(102, true);
    } catch (const std::exception& ex) {
        std::cout << "[Tx #102] Caught exception: " << ex.what() << "\n";
    }

    // 3. TaskGroup with automatic sibling cancellation on failure
    try {
        gorun::TaskGroup group;

        group.spawn([](std::stop_token st) {
            while (!st.stop_requested()) {
                gorun::sleep_for(2ms);
            }
            std::cout << "[TaskGroup] Background heartbeat cleanly stopped via std::stop_token!\n";
        });

        group.spawn([]() {
            gorun::sleep_for(10ms);
            throw std::runtime_error("Upstream service unreachable");
        });

        group.wait();
    } catch (const std::exception& ex) {
        std::cout << "[TaskGroup] Propagated error to parent: " << ex.what() << "\n";
    }

    return 0;
}
