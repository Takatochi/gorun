#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "gorun/gorun.hpp"

using namespace std::chrono_literals;

/**
 * Reference Zero-Dependency Multithreaded HTTP/1.1 Server built purely on `gorun`:
 * - Native non-blocking `gorun::net::TcpListener` & `TcpStream` (IOCP / epoll / kqueue)
 * - Structured Concurrency (`gorun::Scope`) & cascading cancellation (`gorun::Context`)
 * - 1 Green Routine per TCP connection (Compact 64 KB stacks)
 * - Live scheduler telemetry via `gorun::default_runtime().stats()`
 *
 * Usage:
 *   ./example_06_http_server           -> runs built-in Keep-Alive + 200 Concurrent I/O benchmark
 *   ./example_06_http_server --serve   -> serves HTTP/1.1 on http://127.0.0.1:8080
 */

static std::string make_http_response(int status, std::string_view reason,
                                      std::string_view content_type,
                                      std::string_view body,
                                      bool keep_alive) {
    std::string out;
    out.reserve(160 + body.size());
    out += "HTTP/1.1 ";
    out += std::to_string(status);
    out += ' ';
    out += reason;
    out += "\r\nContent-Type: ";
    out += content_type;
    out += "\r\nContent-Length: ";
    out += std::to_string(body.size());
    out += keep_alive ? "\r\nConnection: keep-alive\r\n\r\n" : "\r\nConnection: close\r\n\r\n";
    out += body;
    return out;
}

static void handle_http_connection(gorun::net::TcpStream sock,
                                   gorun::Context srv_ctx,
                                   std::atomic<std::uint64_t>& total_reqs) {
    std::string recv_buf;
    recv_buf.reserve(4096);
    char chunk[4096];

    while (!srv_ctx.is_canceled()) {
        // Read until "\r\n\r\n" header terminator (fast-path non-contentious IOCP/epoll read!)
        std::size_t header_end = recv_buf.find("\r\n\r\n");
        while (header_end == std::string::npos) {
            auto r = sock.read(chunk, sizeof(chunk));
            if (!r.ok() || r.eof()) {
                return;
            }
            recv_buf.append(chunk, static_cast<std::size_t>(r.bytes));
            if (recv_buf.size() > 16 * 1024) {
                (void)sock.write_all(make_http_response(431, "Header Too Large", "text/plain", "too large", false));
                return;
            }
            header_end = recv_buf.find("\r\n\r\n");
        }

        std::string_view header_view(recv_buf.data(), header_end);
        const std::size_t line_end = header_view.find("\r\n");
        std::string_view req_line = (line_end == std::string_view::npos)
            ? header_view
            : header_view.substr(0, line_end);

        // Parse "GET /path HTTP/1.1"
        const std::size_t sp1 = req_line.find(' ');
        const std::size_t sp2 = (sp1 != std::string_view::npos) ? req_line.find(' ', sp1 + 1) : std::string_view::npos;
        if (sp1 == std::string_view::npos || sp2 == std::string_view::npos) {
            (void)sock.write_all(make_http_response(400, "Bad Request", "text/plain", "bad request", false));
            return;
        }
        std::string_view path = req_line.substr(sp1 + 1, sp2 - sp1 - 1);
        const bool keep_alive = header_view.find("Connection: close") == std::string_view::npos &&
                                header_view.find("connection: close") == std::string_view::npos;

        // Consume request headers from buffer
        recv_buf.erase(0, header_end + 4);
        total_reqs.fetch_add(1, std::memory_order_relaxed);

        std::string resp;
        if (path == "/api/ping") {
            resp = make_http_response(200, "OK", "application/json",
                                      "{\"status\":\"ok\",\"runtime\":\"gorun\"}", keep_alive);
        } else if (path == "/api/io-wait") {
            // Simulates 10ms non-blocking downstream I/O — parks green routine without blocking OS workers!
            gorun::sleep_for(10ms);
            resp = make_http_response(200, "OK", "application/json",
                                      "{\"waited_ms\":10}", keep_alive);
        } else if (path == "/stats") {
            auto st = gorun::default_runtime().stats();
            std::string body = "{\"workers\":" + std::to_string(st.worker_count) +
                               ",\"active_routines\":" + std::to_string(st.active_routines) +
                               ",\"parked_routines\":" + std::to_string(st.parked_routines) +
                               ",\"total_steals\":" + std::to_string(st.total_steals) +
                               ",\"pooled_stacks\":" + std::to_string(st.stack_pool.pooled_stacks) +
                               ",\"total_requests\":" + std::to_string(total_reqs.load()) + "}";
            resp = make_http_response(200, "OK", "application/json", body, keep_alive);
        } else {
            resp = make_http_response(404, "Not Found", "application/json",
                                      "{\"error\":\"not_found\"}", keep_alive);
        }

        auto wr = sock.write_all(resp);
        if (!wr.ok() || !keep_alive) {
            return;
        }
    }
}

static void run_self_benchmark() {
    auto listener = gorun::net::TcpListener::bind("127.0.0.1", 0);
    const std::uint16_t port = listener.local_port();
    std::atomic<std::uint64_t> total_reqs{0};

    auto [srv_ctx, srv_cancel] = gorun::with_cancel(gorun::background());
    gorun::WaitGroup srv_wg;
    srv_wg.add(1);

    gorun::go([&]() {
        GORUN_DEFER { srv_wg.done(); };
        for (;;) {
            auto peer = listener.accept(srv_ctx);
            if (!peer) break;
            srv_wg.add(1);
            gorun::go(gorun::StackSize::Compact, [s = std::move(*peer), srv_ctx, &total_reqs, &srv_wg]() mutable {
                GORUN_DEFER { srv_wg.done(); };
                handle_http_connection(std::move(s), srv_ctx, total_reqs);
            });
        }
    });

    std::cout << "====================================================================\n";
    std::cout << "  gorun v0.4.0 Native NetPoller HTTP/1.1 Server Benchmark\n";
    std::cout << "  Arch: " << gorun::detail::kArchitectureName
              << " | Workers: " << gorun::default_runtime().worker_count()
              << " | Port: " << port << "\n";
    std::cout << "====================================================================\n";

    // Benchmark 1: Keep-Alive JSON API (32 concurrent clients x 500 requests = 16,000 requests)
    {
        constexpr int kClients = 32;
        constexpr int kReqsPerClient = 500;
        std::vector<std::vector<double>> per_client_lat(kClients);
        std::atomic<int> ok_count{0};

        auto t0 = std::chrono::steady_clock::now();
        {
            std::vector<std::jthread> clients;
            clients.reserve(kClients);
            for (int c = 0; c < kClients; ++c) {
                clients.emplace_back([&, c]() {
                    auto sock = gorun::net::TcpStream::connect("127.0.0.1", port);
                    per_client_lat[c].reserve(kReqsPerClient);
                    const std::string req = "GET /api/ping HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n";
                    char buf[1024];
                    for (int i = 0; i < kReqsPerClient; ++i) {
                        auto r0 = std::chrono::steady_clock::now();
                        if (!sock.write_all(req).ok()) break;
                        std::string resp;
                        while (resp.find("\r\n\r\n") == std::string::npos) {
                            auto rd = sock.read(buf, sizeof(buf));
                            if (!rd.ok() || rd.eof()) break;
                            resp.append(buf, static_cast<std::size_t>(rd.bytes));
                        }
                        if (resp.find("200 OK") != std::string::npos) {
                            ok_count.fetch_add(1, std::memory_order_relaxed);
                        }
                        auto r1 = std::chrono::steady_clock::now();
                        per_client_lat[c].push_back(
                            std::chrono::duration<double, std::micro>(r1 - r0).count()
                        );
                    }
                });
            }
        }
        auto t1 = std::chrono::steady_clock::now();
        double sec = std::chrono::duration<double>(t1 - t0).count();

        std::vector<double> all_lat;
        for (const auto& v : per_client_lat) all_lat.insert(all_lat.end(), v.begin(), v.end());
        std::sort(all_lat.begin(), all_lat.end());
        double p50 = all_lat.empty() ? 0.0 : all_lat[all_lat.size() / 2];
        double p99 = all_lat.empty() ? 0.0 : all_lat[(all_lat.size() * 99) / 100];

        std::cout << std::fixed << std::setprecision(1);
        std::cout << "[1] Keep-Alive JSON API (32 x 500 = 16,000 reqs):\n";
        std::cout << "    OK: " << ok_count.load() << "/16000 | Throughput: "
                  << (ok_count.load() / sec) << " req/s | p50: " << p50 << " us | p99: " << p99 << " us\n";
    }

    // Benchmark 2: 200 Concurrent I/O-Bound Handlers (10ms sleep each)
    {
        constexpr int kBurst = 200;
        std::atomic<int> ok_count{0};
        auto t0 = std::chrono::steady_clock::now();
        {
            gorun::Scope scope;
            for (int i = 0; i < kBurst; ++i) {
                gorun::go(gorun::StackSize::Compact, [port, &ok_count]() {
                    auto sock = gorun::net::TcpStream::connect("127.0.0.1", port);
                    const std::string req = "GET /api/io-wait HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
                    if (!sock.write_all(req).ok()) return;
                    char buf[512];
                    std::string resp;
                    for (;;) {
                        auto rd = sock.read(buf, sizeof(buf));
                        if (!rd.ok() || rd.eof()) break;
                        resp.append(buf, static_cast<std::size_t>(rd.bytes));
                    }
                    if (resp.find("200 OK") != std::string::npos) {
                        ok_count.fetch_add(1, std::memory_order_relaxed);
                    }
                });
            }
        }
        auto t1 = std::chrono::steady_clock::now();
        double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
        std::cout << "[2] 200 Concurrent I/O Handlers (10ms sleep each):\n";
        std::cout << "    OK: " << ok_count.load() << "/200 | Wall time: " << ms
                  << " ms (" << (ok_count.load() / (ms / 1000.0)) << " req/s) on fixed "
                  << gorun::default_runtime().worker_count() << " OS workers\n";
    }

    auto st = gorun::default_runtime().stats();
    std::cout << "[Stats] spawned=" << st.total_spawned
              << " steals=" << st.total_steals
              << " pooled_stacks=" << st.stack_pool.pooled_stacks
              << " stack_reuses=" << st.stack_pool.total_reused << "\n";

    srv_cancel();
    listener.close();
    srv_wg.wait();
}

int main(int argc, char** argv) {
    if (argc > 1 && std::string_view(argv[1]) == "--serve") {
        auto listener = gorun::net::TcpListener::bind("127.0.0.1", 8080);
        std::atomic<std::uint64_t> total_reqs{0};
        std::cout << "gorun native HTTP/1.1 server listening on http://127.0.0.1:"
                  << listener.local_port() << " (/api/ping, /api/io-wait, /stats)\n";
        auto ctx = gorun::background();
        for (;;) {
            auto peer = listener.accept(ctx);
            if (!peer) break;
            gorun::go(gorun::StackSize::Compact, [s = std::move(*peer), ctx, &total_reqs]() mutable {
                handle_http_connection(std::move(s), ctx, total_reqs);
            });
        }
        return 0;
    }

    run_self_benchmark();
    return 0;
}
