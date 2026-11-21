#pragma once

/**
 * @file net.hpp
 * @brief Zero-dependency native asynchronous Network I/O Poller (`gorun::net`) built directly on
 *        Windows IOCP (`CreateIoCompletionPort`, `WSARecv`, `WSASend`, `AcceptEx`, `ConnectEx`),
 *        Linux `epoll` (`EPOLLET | EPOLLONESHOT`), and macOS/BSD `kqueue` (`EV_ONESHOT`).
 *
 * Parks calling `gorun` green routines while sockets wait for network readiness or completion,
 * leaving all M:N OS worker threads free to serve thousands of other connections. Fully integrated
 * with `gorun::Context` cancellation and deadlines.
 */

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <optional>
#include <semaphore>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <utility>

#include "gorun/channel.hpp"
#include "gorun/context_go.hpp"
#include "gorun/defer.hpp"
#include "gorun/runtime.hpp"

#if defined(_WIN32)
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#include <mswsock.h>
#else
#include <arpa/inet.h>
#include <cerrno>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>
#if defined(__linux__)
#include <sys/epoll.h>
#include <sys/eventfd.h>
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__)
#include <sys/event.h>
#endif
#endif

namespace gorun::net {

/**
 * @struct IoResult
 * @brief Result of an asynchronous socket read or write operation.
 */
struct IoResult {
    std::ptrdiff_t bytes{0};
    std::error_code ec{};

    [[nodiscard]] bool ok() const noexcept { return !ec; }
    [[nodiscard]] bool eof() const noexcept { return !ec && bytes == 0; }
};

namespace detail {

#if defined(_WIN32)

inline std::error_code win_error(DWORD code) noexcept {
    if (code == 0) return {};
    if (code == ERROR_OPERATION_ABORTED || code == WSA_OPERATION_ABORTED) {
        return std::make_error_code(std::errc::operation_canceled);
    }
    if (code == WSAETIMEDOUT || code == WAIT_TIMEOUT) {
        return std::make_error_code(std::errc::timed_out);
    }
    if (code == WSAECONNRESET || code == ERROR_NETNAME_DELETED) {
        return std::make_error_code(std::errc::connection_reset);
    }
    if (code == WSAECONNREFUSED) {
        return std::make_error_code(std::errc::connection_refused);
    }
    return std::error_code(static_cast<int>(code), std::system_category());
}

struct WsaRuntime {
    WsaRuntime() {
        WSADATA wsa{};
        if (::WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
            throw std::runtime_error("gorun::net: WSAStartup failed");
        }
    }
    ~WsaRuntime() {
        ::WSACleanup();
    }
};

inline void ensure_wsa() {
    static WsaRuntime wsa_singleton;
    (void)wsa_singleton;
}

/// Overlapped IOCP operation state; self-retained until the kernel posts its completion packet.
struct IocpOp {
    OVERLAPPED ov{};
    std::shared_ptr<IocpOp> self_ref{};

    ::gorun::detail::SpinLock lock{};
    bool completed{false};
    DWORD bytes_transferred{0};
    DWORD error_code{0};
    ::gorun::detail::UniversalWaiter* waiter{nullptr};
    std::shared_ptr<Channel<bool>> select_ch{};

    void complete(DWORD bytes, DWORD err) noexcept {
        ::gorun::detail::UniversalWaiter* w = nullptr;
        std::shared_ptr<Channel<bool>> ch;
        {
            std::lock_guard g(lock);
            bytes_transferred = bytes;
            error_code = err;
            completed = true;
            w = waiter;
            waiter = nullptr;
            ch = select_ch;
        }
        if (w) {
            w->unpark();
        }
        if (ch) {
            bool v = true;
            bool closed = false;
            (void)ch->try_send(v, closed);
        }
    }

    void wait_uninterruptible() noexcept {
        lock.lock();
        if (completed) {
            lock.unlock();
            return;
        }
        std::binary_semaphore sem{0};
        auto w = ::gorun::detail::UniversalWaiter::make_for_current(sem);
        waiter = &w;
        w.park_current(lock);
    }

    /// Waits for IOCP completion or `ctx` cancellation; on cancel, calls `CancelIoEx` and drains the abort packet safely.
    bool wait_with_context(SOCKET sock, const Context& ctx) {
        if (!ctx.can_cancel()) {
            wait_uninterruptible();
            return false;
        }

        auto ch = std::make_shared<Channel<bool>>(1);
        {
            std::lock_guard g(lock);
            if (completed) return false;
            select_ch = ch;
        }

        bool canceled = false;
        select(
            ch->on_recv([&](bool) {}),
            ctx.on_done([&]() { canceled = true; })
        );

        if (canceled) {
            ::CancelIoEx(reinterpret_cast<HANDLE>(sock), &ov);
            // Must wait for the kernel to finish aborting so caller's buffer is never touched after return!
            wait_uninterruptible();
            return true;
        }
        return false;
    }
};

class IocpPoller {
    static constexpr ULONG_PTR kShutdownKey = 0xDEADBEEFu;

    HANDLE iocp_{nullptr};
    std::jthread poller_thread_;
    LPFN_ACCEPTEX accept_ex_{nullptr};
    LPFN_CONNECTEX connect_ex_{nullptr};

    void load_extension_fns() {
        SOCKET tmp = ::WSASocketW(AF_INET, SOCK_STREAM, IPPROTO_TCP, nullptr, 0, WSA_FLAG_OVERLAPPED);
        if (tmp == INVALID_SOCKET) {
            throw std::runtime_error("gorun::net: failed to create probe socket for Winsock extensions");
        }
        DWORD bytes = 0;
        GUID guid_accept = WSAID_ACCEPTEX;
        ::WSAIoctl(tmp, SIO_GET_EXTENSION_FUNCTION_POINTER,
                   &guid_accept, sizeof(guid_accept),
                   &accept_ex_, sizeof(accept_ex_), &bytes, nullptr, nullptr);

        GUID guid_connect = WSAID_CONNECTEX;
        ::WSAIoctl(tmp, SIO_GET_EXTENSION_FUNCTION_POINTER,
                   &guid_connect, sizeof(guid_connect),
                   &connect_ex_, sizeof(connect_ex_), &bytes, nullptr, nullptr);
        ::closesocket(tmp);
    }

    void poll_loop() {
        for (;;) {
            DWORD bytes = 0;
            ULONG_PTR key = 0;
            LPOVERLAPPED ov = nullptr;
            BOOL ok = ::GetQueuedCompletionStatus(iocp_, &bytes, &key, &ov, INFINITE);

            if (key == kShutdownKey && ov == nullptr) {
                return;
            }
            if (!ov) {
                continue;
            }

            DWORD err = ok ? 0 : ::GetLastError();
            auto* op = reinterpret_cast<IocpOp*>(ov);
            std::shared_ptr<IocpOp> keep_alive = std::move(op->self_ref);
            op->complete(bytes, err);
        }
    }

public:
    IocpPoller() {
        ensure_wsa();
        iocp_ = ::CreateIoCompletionPort(INVALID_HANDLE_VALUE, nullptr, 0, 1);
        if (!iocp_) {
            throw std::runtime_error("gorun::net: CreateIoCompletionPort failed");
        }
        load_extension_fns();
        poller_thread_ = std::jthread([this]() { poll_loop(); });
    }

    ~IocpPoller() {
        if (iocp_) {
            ::PostQueuedCompletionStatus(iocp_, 0, kShutdownKey, nullptr);
            if (poller_thread_.joinable()) {
                poller_thread_.join();
            }
            ::CloseHandle(iocp_);
            iocp_ = nullptr;
        }
    }

    bool associate(SOCKET s) const {
        if (!::CreateIoCompletionPort(reinterpret_cast<HANDLE>(s), iocp_, 0, 0)) {
            throw std::runtime_error("gorun::net: failed to associate socket with IOCP");
        }
        return ::SetFileCompletionNotificationModes(
            reinterpret_cast<HANDLE>(s),
            FILE_SKIP_COMPLETION_PORT_ON_SUCCESS | FILE_SKIP_SET_EVENT_ON_HANDLE
        ) != 0;
    }

    [[nodiscard]] LPFN_ACCEPTEX accept_ex() const noexcept { return accept_ex_; }
    [[nodiscard]] LPFN_CONNECTEX connect_ex() const noexcept { return connect_ex_; }

    static IocpPoller& instance() {
        static IocpPoller poller;
        return poller;
    }
};

#else // POSIX (Linux epoll / macOS & BSD kqueue)

inline void set_nonblocking(int fd) {
    int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags >= 0) {
        ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    }
}

struct PollWaiter {
    ::gorun::detail::SpinLock lock{};
    bool ready{false};
    ::gorun::detail::UniversalWaiter* waiter{nullptr};
    std::shared_ptr<Channel<bool>> select_ch{};

    void wake() noexcept {
        ::gorun::detail::UniversalWaiter* w = nullptr;
        std::shared_ptr<Channel<bool>> ch;
        {
            std::lock_guard g(lock);
            ready = true;
            w = waiter;
            waiter = nullptr;
            ch = select_ch;
        }
        if (w) w->unpark();
        if (ch) {
            bool v = true;
            bool closed = false;
            (void)ch->try_send(v, closed);
        }
    }

    bool wait(const Context& ctx) {
        if (!ctx.can_cancel()) {
            lock.lock();
            if (ready) {
                lock.unlock();
                return false;
            }
            std::binary_semaphore sem{0};
            auto w = ::gorun::detail::UniversalWaiter::make_for_current(sem);
            waiter = &w;
            w.park_current(lock);
            return false;
        }

        auto ch = std::make_shared<Channel<bool>>(1);
        {
            std::lock_guard g(lock);
            if (ready) return false;
            select_ch = ch;
        }

        bool canceled = false;
        select(
            ch->on_recv([&](bool) {}),
            ctx.on_done([&]() { canceled = true; })
        );
        return canceled;
    }
};

class PosixPoller {
    int poll_fd_{-1};
#if defined(__linux__)
    int wake_fd_{-1};
#endif
    std::atomic<bool> stop_{false};
    std::mutex map_mtx_;
    struct FdWaiters {
        std::shared_ptr<PollWaiter> read_w;
        std::shared_ptr<PollWaiter> write_w;
    };
    std::unordered_map<int, FdWaiters> waiters_;
    std::jthread poller_thread_;

    void poll_loop() {
#if defined(__linux__)
        epoll_event events[128];
        while (!stop_.load(std::memory_order_acquire)) {
            int n = ::epoll_wait(poll_fd_, events, 128, -1);
            if (n <= 0) continue;
            for (int i = 0; i < n; ++i) {
                int fd = events[i].data.fd;
                if (fd == wake_fd_) {
                    std::uint64_t val = 0;
                    (void)::read(wake_fd_, &val, sizeof(val));
                    continue;
                }
                const std::uint32_t ev = events[i].events;
                std::shared_ptr<PollWaiter> rw;
                std::shared_ptr<PollWaiter> ww;
                {
                    std::lock_guard lk(map_mtx_);
                    auto it = waiters_.find(fd);
                    if (it != waiters_.end()) {
                        if (ev & (EPOLLIN | EPOLLERR | EPOLLHUP)) {
                            rw = std::move(it->second.read_w);
                        }
                        if (ev & (EPOLLOUT | EPOLLERR | EPOLLHUP)) {
                            ww = std::move(it->second.write_w);
                        }
                    }
                }
                if (rw) rw->wake();
                if (ww) ww->wake();
            }
        }
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__)
        struct kevent events[128];
        while (!stop_.load(std::memory_order_acquire)) {
            int n = ::kevent(poll_fd_, nullptr, 0, events, 128, nullptr);
            if (n <= 0) continue;
            for (int i = 0; i < n; ++i) {
                if (events[i].filter == EVFILT_USER) continue;
                int fd = static_cast<int>(events[i].ident);
                std::shared_ptr<PollWaiter> rw;
                std::shared_ptr<PollWaiter> ww;
                {
                    std::lock_guard lk(map_mtx_);
                    auto it = waiters_.find(fd);
                    if (it != waiters_.end()) {
                        if (events[i].filter == EVFILT_READ) rw = std::move(it->second.read_w);
                        if (events[i].filter == EVFILT_WRITE) ww = std::move(it->second.write_w);
                    }
                }
                if (rw) rw->wake();
                if (ww) ww->wake();
            }
        }
#endif
    }

public:
    PosixPoller() {
#if defined(__linux__)
        poll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
        wake_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (poll_fd_ < 0 || wake_fd_ < 0) {
            throw std::runtime_error("gorun::net: epoll_create1/eventfd failed");
        }
        epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.fd = wake_fd_;
        ::epoll_ctl(poll_fd_, EPOLL_CTL_ADD, wake_fd_, &ev);
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__)
        poll_fd_ = ::kqueue();
        if (poll_fd_ < 0) {
            throw std::runtime_error("gorun::net: kqueue() failed");
        }
        struct kevent kev{};
        EV_SET(&kev, 1, EVFILT_USER, EV_ADD | EV_CLEAR, 0, 0, nullptr);
        ::kevent(poll_fd_, &kev, 1, nullptr, 0, nullptr);
#endif
        poller_thread_ = std::jthread([this]() { poll_loop(); });
    }

    ~PosixPoller() {
        stop_.store(true, std::memory_order_release);
#if defined(__linux__)
        std::uint64_t one = 1;
        (void)::write(wake_fd_, &one, sizeof(one));
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__)
        struct kevent kev{};
        EV_SET(&kev, 1, EVFILT_USER, 0, NOTE_TRIGGER, 0, nullptr);
        ::kevent(poll_fd_, &kev, 1, nullptr, 0, nullptr);
#endif
        if (poller_thread_.joinable()) {
            poller_thread_.join();
        }
#if defined(__linux__)
        if (wake_fd_ >= 0) ::close(wake_fd_);
#endif
        if (poll_fd_ >= 0) ::close(poll_fd_);
    }

    void unregister_fd(int fd) {
        std::shared_ptr<PollWaiter> rw;
        std::shared_ptr<PollWaiter> ww;
        {
            std::lock_guard lk(map_mtx_);
            auto it = waiters_.find(fd);
            if (it != waiters_.end()) {
                rw = std::move(it->second.read_w);
                ww = std::move(it->second.write_w);
                waiters_.erase(it);
            }
        }
#if defined(__linux__)
        ::epoll_ctl(poll_fd_, EPOLL_CTL_DEL, fd, nullptr);
#endif
        if (rw) rw->wake();
        if (ww) ww->wake();
    }

    bool wait_ready(int fd, bool for_write, const Context& ctx) {
        auto w = std::make_shared<PollWaiter>();
        {
            std::lock_guard lk(map_mtx_);
            auto& slot = waiters_[fd];
            if (for_write) slot.write_w = w;
            else slot.read_w = w;

#if defined(__linux__)
            epoll_event ev{};
            ev.data.fd = fd;
            ev.events = EPOLLET | EPOLLONESHOT;
            if (slot.read_w) ev.events |= EPOLLIN;
            if (slot.write_w) ev.events |= EPOLLOUT;
            if (::epoll_ctl(poll_fd_, EPOLL_CTL_MOD, fd, &ev) != 0 && errno == ENOENT) {
                ::epoll_ctl(poll_fd_, EPOLL_CTL_ADD, fd, &ev);
            }
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__)
            struct kevent kev{};
            EV_SET(&kev, static_cast<uintptr_t>(fd),
                   for_write ? EVFILT_WRITE : EVFILT_READ,
                   EV_ADD | EV_ONESHOT, 0, 0, nullptr);
            ::kevent(poll_fd_, &kev, 1, nullptr, 0, nullptr);
#endif
        }
        bool canceled = w->wait(ctx);
        if (canceled) {
            std::lock_guard lk(map_mtx_);
            auto it = waiters_.find(fd);
            if (it != waiters_.end()) {
                if (for_write && it->second.write_w == w) it->second.write_w.reset();
                if (!for_write && it->second.read_w == w) it->second.read_w.reset();
            }
        }
        return canceled;
    }

    static PosixPoller& instance() {
        static PosixPoller poller;
        return poller;
    }
};

#endif

} // namespace detail

/**
 * @class TcpStream
 * @brief Non-blocking, green-routine-friendly TCP stream backed by IOCP (Windows), epoll (Linux), or kqueue (macOS).
 */
class TcpStream {
#if defined(_WIN32)
    SOCKET sock_{INVALID_SOCKET};
    bool skip_iocp_on_sync_{false};
#else
    int sock_{-1};
#endif
    std::string peer_addr_{};

    friend class TcpListener;

#if defined(_WIN32)
    TcpStream(SOCKET s, std::string peer, bool skip_sync = false) noexcept
        : sock_(s), skip_iocp_on_sync_(skip_sync), peer_addr_(std::move(peer)) {}
#else
    TcpStream(int s, std::string peer) noexcept : sock_(s), peer_addr_(std::move(peer)) {}
#endif

public:
    TcpStream() noexcept = default;

    TcpStream(TcpStream&& other) noexcept
        : sock_(std::exchange(other.sock_,
#if defined(_WIN32)
                              INVALID_SOCKET
#else
                              -1
#endif
                              ))
#if defined(_WIN32)
        , skip_iocp_on_sync_(std::exchange(other.skip_iocp_on_sync_, false))
#endif
        , peer_addr_(std::move(other.peer_addr_)) {}

    TcpStream& operator=(TcpStream&& other) noexcept {
        if (this != &other) {
            close();
            sock_ = std::exchange(other.sock_,
#if defined(_WIN32)
                                  INVALID_SOCKET
#else
                                  -1
#endif
            );
#if defined(_WIN32)
            skip_iocp_on_sync_ = std::exchange(other.skip_iocp_on_sync_, false);
#endif
            peer_addr_ = std::move(other.peer_addr_);
        }
        return *this;
    }

    TcpStream(const TcpStream&) = delete;
    TcpStream& operator=(const TcpStream&) = delete;

    ~TcpStream() {
        close();
    }

    [[nodiscard]] bool is_open() const noexcept {
#if defined(_WIN32)
        return sock_ != INVALID_SOCKET;
#else
        return sock_ >= 0;
#endif
    }

    [[nodiscard]] const std::string& peer_address() const noexcept { return peer_addr_; }

    void set_nodelay(bool enable) noexcept {
        if (!is_open()) return;
        int flag = enable ? 1 : 0;
#if defined(_WIN32)
        ::setsockopt(sock_, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&flag), sizeof(flag));
#else
        ::setsockopt(sock_, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
#endif
    }

    void shutdown_write() noexcept {
        if (!is_open()) return;
#if defined(_WIN32)
        ::shutdown(sock_, SD_SEND);
#else
        ::shutdown(sock_, SHUT_WR);
#endif
    }

    void close() noexcept {
        if (!is_open()) return;
#if defined(_WIN32)
        SOCKET s = std::exchange(sock_, INVALID_SOCKET);
        ::CancelIoEx(reinterpret_cast<HANDLE>(s), nullptr);
        ::closesocket(s);
#else
        int s = std::exchange(sock_, -1);
        detail::PosixPoller::instance().unregister_fd(s);
        ::close(s);
#endif
    }

    /**
     * @brief Establishes a non-blocking TCP connection to `host:port` without blocking M:N worker threads.
     */
    static TcpStream connect(std::string_view host, std::uint16_t port, Context ctx = background()) {
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        std::string host_str(host);
        if (::inet_pton(AF_INET, host_str.c_str(), &addr.sin_addr) != 1) {
            throw std::runtime_error("gorun::net::TcpStream::connect: invalid IPv4 address: " + host_str);
        }

#if defined(_WIN32)
        auto& poller = detail::IocpPoller::instance();
        SOCKET s = ::WSASocketW(AF_INET, SOCK_STREAM, IPPROTO_TCP, nullptr, 0, WSA_FLAG_OVERLAPPED);
        if (s == INVALID_SOCKET) {
            throw std::runtime_error("gorun::net::TcpStream::connect: WSASocket failed");
        }
        // ConnectEx requires the socket to be bound to an ephemeral local port first
        sockaddr_in local{};
        local.sin_family = AF_INET;
        local.sin_addr.s_addr = INADDR_ANY;
        local.sin_port = 0;
        if (::bind(s, reinterpret_cast<const sockaddr*>(&local), sizeof(local)) == SOCKET_ERROR) {
            ::closesocket(s);
            throw std::runtime_error("gorun::net::TcpStream::connect: bind failed");
        }
        bool skip_sync = poller.associate(s);

        auto op = std::make_shared<detail::IocpOp>();
        op->self_ref = op;
        DWORD sent = 0;
        BOOL ok = poller.connect_ex()(
            s, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr),
            nullptr, 0, &sent, &op->ov
        );
        if (ok && skip_sync) {
            op->self_ref.reset();
        } else {
            if (!ok) {
                DWORD err = ::WSAGetLastError();
                if (err != WSA_IO_PENDING) {
                    op->self_ref.reset();
                    ::closesocket(s);
                    throw std::system_error(detail::win_error(err), "TcpStream::connect failed");
                }
            }
            bool canceled = op->wait_with_context(s, ctx);
            if (canceled || op->error_code != 0) {
                ::closesocket(s);
                throw std::system_error(
                    canceled ? std::make_error_code(std::errc::operation_canceled) : detail::win_error(op->error_code),
                    "TcpStream::connect failed"
                );
            }
        }
        ::setsockopt(s, SOL_SOCKET, SO_UPDATE_CONNECT_CONTEXT, nullptr, 0);
        TcpStream stream(s, host_str + ":" + std::to_string(port), skip_sync);
        stream.set_nodelay(true);
        return stream;
#else
        int s = ::socket(AF_INET, SOCK_STREAM, 0);
        if (s < 0) {
            throw std::system_error(errno, std::generic_category(), "TcpStream::connect socket()");
        }
        detail::set_nonblocking(s);
        int rc = ::connect(s, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr));
        if (rc < 0) {
            if (errno != EINPROGRESS) {
                int err = errno;
                ::close(s);
                throw std::system_error(err, std::generic_category(), "TcpStream::connect()");
            }
            bool canceled = detail::PosixPoller::instance().wait_ready(s, /*for_write=*/true, ctx);
            if (canceled) {
                detail::PosixPoller::instance().unregister_fd(s);
                ::close(s);
                throw std::system_error(std::make_error_code(std::errc::operation_canceled), "TcpStream::connect");
            }
            int so_err = 0;
            socklen_t len = sizeof(so_err);
            ::getsockopt(s, SOL_SOCKET, SO_ERROR, &so_err, &len);
            if (so_err != 0) {
                detail::PosixPoller::instance().unregister_fd(s);
                ::close(s);
                throw std::system_error(so_err, std::generic_category(), "TcpStream::connect");
            }
        }
        TcpStream stream(s, host_str + ":" + std::to_string(port));
        stream.set_nodelay(true);
        return stream;
#endif
    }

    /**
     * @brief Reads up to `len` bytes into `buf`, parking the calling green routine until data arrives or `ctx` cancels.
     */
    IoResult read(void* buf, std::size_t len, Context ctx = background()) {
        if (!is_open()) {
            return IoResult{0, std::make_error_code(std::errc::bad_file_descriptor)};
        }
        if (ctx.is_canceled()) {
            return IoResult{0, std::make_error_code(std::errc::operation_canceled)};
        }

#if defined(_WIN32)
        auto op = std::make_shared<detail::IocpOp>();
        op->self_ref = op;
        WSABUF wbuf{};
        wbuf.buf = static_cast<CHAR*>(buf);
        wbuf.len = static_cast<ULONG>(len);
        DWORD flags = 0;
        DWORD recvd = 0;
        int rc = ::WSARecv(sock_, &wbuf, 1, &recvd, &flags, &op->ov, nullptr);
        if (rc == 0 && skip_iocp_on_sync_) {
            op->self_ref.reset();
            return IoResult{static_cast<std::ptrdiff_t>(recvd), {}};
        }
        if (rc == SOCKET_ERROR) {
            DWORD err = ::WSAGetLastError();
            if (err != WSA_IO_PENDING) {
                op->self_ref.reset();
                return IoResult{0, detail::win_error(err)};
            }
        }
        bool canceled = op->wait_with_context(sock_, ctx);
        if (canceled) {
            return IoResult{0, std::make_error_code(std::errc::operation_canceled)};
        }
        if (op->error_code != 0) {
            return IoResult{0, detail::win_error(op->error_code)};
        }
        return IoResult{static_cast<std::ptrdiff_t>(op->bytes_transferred), {}};
#else
        for (;;) {
            ssize_t n = ::recv(sock_, buf, len, 0);
            if (n >= 0) {
                return IoResult{static_cast<std::ptrdiff_t>(n), {}};
            }
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                if (detail::PosixPoller::instance().wait_ready(sock_, /*for_write=*/false, ctx)) {
                    return IoResult{0, std::make_error_code(std::errc::operation_canceled)};
                }
                continue;
            }
            return IoResult{0, std::error_code(errno, std::generic_category())};
        }
#endif
    }

    IoResult read(void* buf, std::size_t len, std::chrono::milliseconds timeout) {
        auto [ctx, cancel] = with_timeout(background(), timeout);
        GORUN_DEFER { cancel(); };
        auto res = read(buf, len, ctx);
        if (res.ec == std::errc::operation_canceled && ctx.is_canceled()) {
            res.ec = std::make_error_code(std::errc::timed_out);
        }
        return res;
    }

    /**
     * @brief Writes up to `len` bytes from `buf`, parking the green routine until the kernel accepts data.
     */
    IoResult write(const void* buf, std::size_t len, Context ctx = background()) {
        if (!is_open()) {
            return IoResult{0, std::make_error_code(std::errc::bad_file_descriptor)};
        }
        if (ctx.is_canceled()) {
            return IoResult{0, std::make_error_code(std::errc::operation_canceled)};
        }

#if defined(_WIN32)
        auto op = std::make_shared<detail::IocpOp>();
        op->self_ref = op;
        WSABUF wbuf{};
        wbuf.buf = const_cast<CHAR*>(static_cast<const CHAR*>(buf));
        wbuf.len = static_cast<ULONG>(len);
        DWORD sent = 0;
        int rc = ::WSASend(sock_, &wbuf, 1, &sent, 0, &op->ov, nullptr);
        if (rc == 0 && skip_iocp_on_sync_) {
            op->self_ref.reset();
            return IoResult{static_cast<std::ptrdiff_t>(sent), {}};
        }
        if (rc == SOCKET_ERROR) {
            DWORD err = ::WSAGetLastError();
            if (err != WSA_IO_PENDING) {
                op->self_ref.reset();
                return IoResult{0, detail::win_error(err)};
            }
        }
        bool canceled = op->wait_with_context(sock_, ctx);
        if (canceled) {
            return IoResult{0, std::make_error_code(std::errc::operation_canceled)};
        }
        if (op->error_code != 0) {
            return IoResult{0, detail::win_error(op->error_code)};
        }
        return IoResult{static_cast<std::ptrdiff_t>(op->bytes_transferred), {}};
#else
        for (;;) {
#if defined(MSG_NOSIGNAL)
            ssize_t n = ::send(sock_, buf, len, MSG_NOSIGNAL);
#else
            ssize_t n = ::send(sock_, buf, len, 0);
#endif
            if (n >= 0) {
                return IoResult{static_cast<std::ptrdiff_t>(n), {}};
            }
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                if (detail::PosixPoller::instance().wait_ready(sock_, /*for_write=*/true, ctx)) {
                    return IoResult{0, std::make_error_code(std::errc::operation_canceled)};
                }
                continue;
            }
            return IoResult{0, std::error_code(errno, std::generic_category())};
        }
#endif
    }

    /// Writes the entire `data` slice until all bytes are sent or an error/cancellation occurs.
    IoResult write_all(std::string_view data, Context ctx = background()) {
        std::size_t total = 0;
        while (total < data.size()) {
            auto r = write(data.data() + total, data.size() - total, ctx);
            if (!r.ok()) {
                return IoResult{static_cast<std::ptrdiff_t>(total), r.ec};
            }
            if (r.bytes == 0) {
                return IoResult{static_cast<std::ptrdiff_t>(total), std::make_error_code(std::errc::broken_pipe)};
            }
            total += static_cast<std::size_t>(r.bytes);
        }
        return IoResult{static_cast<std::ptrdiff_t>(total), {}};
    }

    IoResult write_all(std::string_view data, std::chrono::milliseconds timeout) {
        auto [ctx, cancel] = with_timeout(background(), timeout);
        GORUN_DEFER { cancel(); };
        auto res = write_all(data, ctx);
        if (res.ec == std::errc::operation_canceled && ctx.is_canceled()) {
            res.ec = std::make_error_code(std::errc::timed_out);
        }
        return res;
    }
};

/**
 * @class TcpListener
 * @brief Non-blocking TCP acceptor parking calling green routines on IOCP / epoll / kqueue until a peer connects.
 */
class TcpListener {
#if defined(_WIN32)
    SOCKET listen_sock_{INVALID_SOCKET};
    bool skip_iocp_on_sync_{false};
#else
    int listen_sock_{-1};
#endif
    std::uint16_t bound_port_{0};

public:
    TcpListener() noexcept = default;

    TcpListener(TcpListener&& other) noexcept
        : listen_sock_(std::exchange(other.listen_sock_,
#if defined(_WIN32)
                                     INVALID_SOCKET
#else
                                     -1
#endif
                                     ))
#if defined(_WIN32)
        , skip_iocp_on_sync_(std::exchange(other.skip_iocp_on_sync_, false))
#endif
        , bound_port_(std::exchange(other.bound_port_, 0)) {}

    TcpListener& operator=(TcpListener&& other) noexcept {
        if (this != &other) {
            close();
            listen_sock_ = std::exchange(other.listen_sock_,
#if defined(_WIN32)
                                         INVALID_SOCKET
#else
                                         -1
#endif
            );
#if defined(_WIN32)
            skip_iocp_on_sync_ = std::exchange(other.skip_iocp_on_sync_, false);
#endif
            bound_port_ = std::exchange(other.bound_port_, 0);
        }
        return *this;
    }

    TcpListener(const TcpListener&) = delete;
    TcpListener& operator=(const TcpListener&) = delete;

    ~TcpListener() {
        close();
    }

    [[nodiscard]] std::uint16_t local_port() const noexcept { return bound_port_; }

    void close() noexcept {
#if defined(_WIN32)
        if (listen_sock_ != INVALID_SOCKET) {
            SOCKET s = std::exchange(listen_sock_, INVALID_SOCKET);
            ::CancelIoEx(reinterpret_cast<HANDLE>(s), nullptr);
            ::closesocket(s);
        }
#else
        if (listen_sock_ >= 0) {
            int s = std::exchange(listen_sock_, -1);
            detail::PosixPoller::instance().unregister_fd(s);
            ::close(s);
        }
#endif
    }

    /**
     * @brief Binds and listens on `host:port` (pass `port = 0` for an OS-assigned ephemeral port).
     */
    static TcpListener bind(std::string_view host, std::uint16_t port, int backlog = 512) {
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        std::string host_str(host);
        if (host_str.empty() || host_str == "0.0.0.0") {
            addr.sin_addr.s_addr = INADDR_ANY;
        } else if (::inet_pton(AF_INET, host_str.c_str(), &addr.sin_addr) != 1) {
            throw std::runtime_error("gorun::net::TcpListener::bind: invalid IPv4 address: " + host_str);
        }

#if defined(_WIN32)
        auto& poller = detail::IocpPoller::instance();
        SOCKET s = ::WSASocketW(AF_INET, SOCK_STREAM, IPPROTO_TCP, nullptr, 0, WSA_FLAG_OVERLAPPED);
        if (s == INVALID_SOCKET) {
            throw std::runtime_error("gorun::net::TcpListener::bind: WSASocket failed");
        }
        int reuse = 1;
        ::setsockopt(s, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&reuse), sizeof(reuse));
        if (::bind(s, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) == SOCKET_ERROR) {
            DWORD err = ::WSAGetLastError();
            ::closesocket(s);
            throw std::system_error(detail::win_error(err), "TcpListener::bind failed");
        }
        if (::listen(s, backlog) == SOCKET_ERROR) {
            DWORD err = ::WSAGetLastError();
            ::closesocket(s);
            throw std::system_error(detail::win_error(err), "TcpListener::listen failed");
        }
        bool skip_sync = poller.associate(s);

        sockaddr_in actual{};
        int actual_len = sizeof(actual);
        ::getsockname(s, reinterpret_cast<sockaddr*>(&actual), &actual_len);

        TcpListener listener;
        listener.listen_sock_ = s;
        listener.skip_iocp_on_sync_ = skip_sync;
        listener.bound_port_ = ntohs(actual.sin_port);
        return listener;
#else
        int s = ::socket(AF_INET, SOCK_STREAM, 0);
        if (s < 0) {
            throw std::system_error(errno, std::generic_category(), "TcpListener::bind socket()");
        }
        int reuse = 1;
        ::setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
        detail::set_nonblocking(s);
        if (::bind(s, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) != 0) {
            int err = errno;
            ::close(s);
            throw std::system_error(err, std::generic_category(), "TcpListener::bind()");
        }
        if (::listen(s, backlog) != 0) {
            int err = errno;
            ::close(s);
            throw std::system_error(err, std::generic_category(), "TcpListener::listen()");
        }
        sockaddr_in actual{};
        socklen_t actual_len = sizeof(actual);
        ::getsockname(s, reinterpret_cast<sockaddr*>(&actual), &actual_len);

        TcpListener listener;
        listener.listen_sock_ = s;
        listener.bound_port_ = ntohs(actual.sin_port);
        return listener;
#endif
    }

    /**
     * @brief Accepts the next incoming TCP connection, parking the green routine until a peer connects
     *        or `ctx` is canceled / listener is closed (returns `std::nullopt`).
     */
    std::optional<TcpStream> accept(Context ctx = background()) {
#if defined(_WIN32)
        if (listen_sock_ == INVALID_SOCKET || ctx.is_canceled()) {
            return std::nullopt;
        }
        auto& poller = detail::IocpPoller::instance();
        SOCKET client = ::WSASocketW(AF_INET, SOCK_STREAM, IPPROTO_TCP, nullptr, 0, WSA_FLAG_OVERLAPPED);
        if (client == INVALID_SOCKET) {
            return std::nullopt;
        }

        // Address buffer for AcceptEx (local + remote sockaddr_storage + 16 bytes each)
        alignas(8) char addr_buf[(sizeof(sockaddr_in) + 16) * 2]{};
        auto op = std::make_shared<detail::IocpOp>();
        op->self_ref = op;
        DWORD recvd = 0;

        BOOL ok = poller.accept_ex()(
            listen_sock_, client, addr_buf, 0,
            sizeof(sockaddr_in) + 16, sizeof(sockaddr_in) + 16,
            &recvd, &op->ov
        );
        if (ok && skip_iocp_on_sync_) {
            op->self_ref.reset();
        } else {
            if (!ok) {
                DWORD err = ::WSAGetLastError();
                if (err != WSA_IO_PENDING) {
                    op->self_ref.reset();
                    ::closesocket(client);
                    return std::nullopt;
                }
            }

            bool canceled = op->wait_with_context(listen_sock_, ctx);
            if (canceled || op->error_code != 0) {
                ::closesocket(client);
                return std::nullopt;
            }
        }

        ::setsockopt(client, SOL_SOCKET, SO_UPDATE_ACCEPT_CONTEXT,
                     reinterpret_cast<const char*>(&listen_sock_), sizeof(listen_sock_));
        bool client_skip_sync = poller.associate(client);

        sockaddr_in peer{};
        int peer_len = sizeof(peer);
        std::string peer_str = "unknown";
        if (::getpeername(client, reinterpret_cast<sockaddr*>(&peer), &peer_len) == 0) {
            char ip[INET_ADDRSTRLEN]{};
            if (::inet_ntop(AF_INET, &peer.sin_addr, ip, sizeof(ip))) {
                peer_str = std::string(ip) + ":" + std::to_string(ntohs(peer.sin_port));
            }
        }

        TcpStream stream(client, std::move(peer_str), client_skip_sync);
        stream.set_nodelay(true);
        return stream;
#else
        if (listen_sock_ < 0 || ctx.is_canceled()) {
            return std::nullopt;
        }
        for (;;) {
            sockaddr_in peer{};
            socklen_t peer_len = sizeof(peer);
            int client = ::accept(listen_sock_, reinterpret_cast<sockaddr*>(&peer), &peer_len);
            if (client >= 0) {
                detail::set_nonblocking(client);
                char ip[INET_ADDRSTRLEN]{};
                std::string peer_str = "unknown";
                if (::inet_ntop(AF_INET, &peer.sin_addr, ip, sizeof(ip))) {
                    peer_str = std::string(ip) + ":" + std::to_string(ntohs(peer.sin_port));
                }
                TcpStream stream(client, std::move(peer_str));
                stream.set_nodelay(true);
                return stream;
            }
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                if (detail::PosixPoller::instance().wait_ready(listen_sock_, /*for_write=*/false, ctx)) {
                    return std::nullopt;
                }
                if (listen_sock_ < 0) return std::nullopt;
                continue;
            }
            return std::nullopt;
        }
#endif
    }
};

} // namespace gorun::net
