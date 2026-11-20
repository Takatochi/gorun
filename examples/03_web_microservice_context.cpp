/**
 * @file 03_web_microservice_context.cpp
 * @brief Example 3: Building an HTTP / Microservice Request Pipeline with `gorun::Context`,
 *        Cascading Timeouts (`with_timeout`), Request Metadata (`with_value`),
 *        and Concurrency Limiting (`gorun::Semaphore`).
 *
 * Shows how a future C++ Web Framework built on `gorun` handles 1-routine-per-request
 * with automatic fan-out SLA deadlines and zero OS-thread blocking.
 */

#include <chrono>
#include <iostream>
#include <mutex>
#include <string>
#include <vector>

#include "gorun/gorun.hpp"

using namespace std::chrono_literals;

struct HttpResponse {
    int status_code;
    std::string body;
};

// Simulated downstream microservice call (e.g. PostgreSQL or Redis query) that respects gorun::Context
std::optional<std::string> query_database_with_context(
    const gorun::Context& ctx,
    gorun::Semaphore& db_pool_limiter,
    std::chrono::milliseconds simulated_latency
) {
    // Limit max concurrent DB queries using a non-OS-blocking green Semaphore
    db_pool_limiter.acquire();
    GORUN_DEFER { db_pool_limiter.release(); };

    gorun::Channel<std::string> row_ch(1);
    gorun::go([row_ch_ptr = &row_ch, ctx, simulated_latency]() {
        gorun::select(
            gorun::on_timeout(simulated_latency, [row_ch_ptr]() {
                row_ch_ptr->send(R"({"user": "Artur Havor", "role": "Admin"})");
            }),
            ctx.on_done([]() {
                // Aborted early when request context expired!
            })
        );
    });

    std::optional<std::string> result;
    gorun::select(
        row_ch.on_recv([&](std::string json) {
            result = std::move(json);
        }),
        ctx.on_done([&]() {
            // Context was canceled or timed out before the DB query finished!
            result = std::nullopt;
        })
    );
    return result;
}

// Simulated HTTP Route Handler (runs inside its own green routine per incoming request)
HttpResponse handle_get_profile(
    const gorun::Context& incoming_ctx,
    gorun::Semaphore& db_pool_limiter,
    std::chrono::milliseconds db_latency
) {
    // Enforce a strict 25ms SLA timeout for this HTTP endpoint
    auto [req_ctx, cancel] = gorun::with_timeout(incoming_ctx, 25ms);
    GORUN_DEFER { cancel(); };

    auto trace_id = req_ctx.value<std::string>("trace_id").value_or("unknown");

    if (auto json = query_database_with_context(req_ctx, db_pool_limiter, db_latency)) {
        return HttpResponse{200, "[trace=" + trace_id + "] 200 OK: " + *json};
    }

    return HttpResponse{
        504,
        "[trace=" + trace_id + "] 504 Gateway Timeout: " + std::string(gorun::to_string(*req_ctx.err()))
    };
}

int main() {
    std::cout << "=== Example 03: Web Microservice Handler with gorun::Context & Semaphore ===\n";

    gorun::Semaphore db_connection_pool(4); // Max 4 concurrent DB queries
    gorun::Mutex cout_mtx;

    // Simulate 4 concurrent incoming HTTP requests (2 fast DB queries, 2 slow queries exceeding the 25ms SLA)
    {
        gorun::Scope server_scope;

        for (int req_id = 1; req_id <= 4; ++req_id) {
            gorun::go([&, req_id]() {
                // Attach per-request Trace ID and Client IP to the request Context
                auto ctx = gorun::with_value(gorun::background(), "trace_id", "req-" + std::to_string(1000 + req_id));
                auto simulated_db_time = (req_id % 2 == 1) ? 8ms : 60ms;

                HttpResponse resp = handle_get_profile(ctx, db_connection_pool, simulated_db_time);

                std::lock_guard<gorun::Mutex> lock(cout_mtx);
                std::cout << "HTTP Response #" << req_id << " -> " << resp.body << "\n";
            });
        }
    }

    return 0;
}
