/**
 * @file demo.cpp
 * @brief Quick-start demonstration of `gorun`: ergonomic `go(...)`, `GORUN_DEFER`, `Channel<T>`,
 *        structured `Scope` (safe `[&]` reference captures), `Mutex`, and `select(...)` multiplexing.
 */

#include <chrono>
#include <iostream>
#include <mutex>
#include <string>
#include <vector>

#include "gorun/gorun.hpp"

using namespace std::chrono_literals;

int main() {
    // 1. Ergonomic launch & buffered channel directly in main() + GORUN_DEFER
    gorun::Channel<int> ch(8);

    gorun::go([&]() {
        GORUN_DEFER { ch.close(); }; // Guaranteed channel close when the routine exits!
        ch.send(42);
    });

    gorun::go([&]() {
        if (auto v = ch.recv()) {
            std::cout << "[Demo 1] Received from channel: " << *v << "\n";
        }
    });

    // Wait for background detached routines to finish
    gorun::default_runtime().wait_idle();

    // 2. Structured Concurrency (Scope) where [&] reference capture is 100% safe
    std::vector<int> results(4, 0);
    {
        gorun::Scope scope; // ~Scope() automatically joins all child go(...) calls!

        for (int i = 0; i < 4; ++i) {
            gorun::go([&, i]() {
                gorun::sleep_for(10ms);
                results[i] = (i + 1) * 100;
            });
        }
    } // <-- All 4 routines are guaranteed to have finished here!

    std::cout << "[Demo 2] Results after leaving Scope: ";
    for (int r : results) {
        std::cout << r << " ";
    }
    std::cout << "\n";

    // 3. Using gorun::Mutex with standard std::lock_guard and gorun::select(...)
    gorun::Mutex log_mtx;
    gorun::Channel<std::string> fast_ch(1);
    gorun::Channel<std::string> slow_ch(1);

    gorun::go([&]() {
        gorun::sleep_for(5ms);
        fast_ch.send("Fast response from service A");
    });

    gorun::go([&]() {
        gorun::sleep_for(50ms);
        slow_ch.send("Slow response from service B");
    });

    gorun::select(
        fast_ch.on_recv([&](std::string msg) {
            std::lock_guard lock(log_mtx);
            std::cout << "[Demo 3] select() picked fast_ch: " << msg << "\n";
        }),
        slow_ch.on_recv([&](std::string msg) {
            std::lock_guard lock(log_mtx);
            std::cout << "[Demo 3] select() picked slow_ch: " << msg << "\n";
        }),
        gorun::on_timeout(100ms, [&]() {
            std::cout << "[Demo 3] Timeout!\n";
        })
    );

    gorun::default_runtime().wait_idle();
    return 0;
}
