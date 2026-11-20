/**
 * @file 01_channels_and_select.cpp
 * @brief Example 1: Go-style Buffered & Unbuffered Channels, Producer-Consumer Fan-Out,
 *        `GORUN_DEFER { ch.close(); }`, and Multi-Channel Multiplexing (`gorun::select`).
 */

#include <chrono>
#include <iostream>
#include <string>

#include "gorun/gorun.hpp"

using namespace std::chrono_literals;

int main() {
    std::cout << "=== Example 01: Channels, GORUN_DEFER & select(...) ===\n";

    // 1. Unbuffered Rendezvous Channel (capacity = 0): direct stack-to-stack handoff
    gorun::Channel<std::string> ping_ch(0);
    gorun::Channel<std::string> pong_ch(0);

    gorun::go([&]() {
        GORUN_DEFER { ping_ch.close(); };
        ping_ch.send("PING from green routine #1");
    });

    gorun::go([&]() {
        GORUN_DEFER { pong_ch.close(); };
        if (auto msg = ping_ch.recv()) {
            pong_ch.send(*msg + " -> PONG from green routine #2");
        }
    });

    if (auto reply = pong_ch.recv()) {
        std::cout << "[Rendezvous] " << *reply << "\n";
    }

    // 2. Multi-Channel select(...) with fast/slow workers and timeout
    gorun::Channel<int> telemetry_ch(4);
    gorun::Channel<std::string> alert_ch(1);

    gorun::go([&]() {
        GORUN_DEFER { telemetry_ch.close(); };
        for (int i = 1; i <= 3; ++i) {
            gorun::sleep_for(5ms);
            telemetry_ch.send(i * 100);
        }
    });

    gorun::go([&]() {
        gorun::sleep_for(12ms);
        alert_ch.send("CPU load spike detected on node-03");
    });

    for (int step = 0; step < 4; ++step) {
        gorun::select(
            telemetry_ch.on_recv([](int metric) {
                std::cout << "[select] Telemetry metric: " << metric << " req/s\n";
            }),
            alert_ch.on_recv([](std::string alert) {
                std::cout << "[select] ALERT: " << alert << "\n";
            }),
            gorun::on_timeout(50ms, []() {
                std::cout << "[select] Timed out waiting for events\n";
            })
        );
    }

    gorun::default_runtime().wait_idle();
    return 0;
}
