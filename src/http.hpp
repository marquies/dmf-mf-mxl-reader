// Reading a request and writing a response on mxl-reader's HTTP socket, and
// the one request it makes itself, to an NMOS registry. Kept apart so it can
// be tested over a real connection.
//
// Both are bounded in time as a whole, not per call, because requests are
// answered one at a time: a client that trickles bytes in or reads the answer
// slowly must not hold the probes queued behind it.

#pragma once

#include <netdb.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <string>
#include <string_view>

namespace reader
{
    using Clock = std::chrono::steady_clock;

    // Waits until the socket is ready for events or the deadline passes.
    inline bool ready(int fd, short events, Clock::time_point deadline)
    {
        auto const left = std::chrono::ceil<std::chrono::milliseconds>(deadline - Clock::now()).count();
        auto pfd = pollfd{.fd = fd, .events = events, .revents = 0};
        return left > 0 && ::poll(&pfd, 1, static_cast<int>(left)) > 0;
    }

    // All of data, or false once the budget is spent or the peer is gone.
    inline bool sendAll(int client, std::string_view data, std::chrono::milliseconds budget)
    {
        auto const deadline = Clock::now() + budget;
        while (!data.empty() && ready(client, POLLOUT, deadline))
        {
            auto const n = ::send(client, data.data(), data.size(), MSG_NOSIGNAL | MSG_DONTWAIT);
            if (n <= 0)
            {
                return false;
            }
            data.remove_prefix(static_cast<std::size_t>(n));
        }
        return data.empty();
    }

    // The value of a header in a request or response head, matched without
    // regard to case; empty when absent.
    inline std::string header(std::string_view head, std::string_view name)
    {
        auto lower = [](std::string_view s) {
            auto out = std::string{s};
            for (auto& c : out)
            {
                c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
            }
            return out;
        };
        auto const h = lower(head);
        auto const key = "\r\n" + lower(name) + ":";
        auto const at = h.find(key);
        if (at == std::string::npos)
        {
            return {};
        }
        auto from = at + key.size();
        auto const end = head.find("\r\n", from);
        auto value = std::string{head.substr(from, end == std::string_view::npos ? std::string_view::npos : end - from)};
        auto const first = value.find_first_not_of(" \t");
        auto const last = value.find_last_not_of(" \t");
        return first == std::string::npos ? std::string{} : value.substr(first, last - first + 1);
    }

    // The request up to the end of its headers, and its body when it states
    // a Content-Length. Closing a socket with unread bytes in it resets the
    // connection, and a reset drops whatever of the response is still unsent;
    // a proxy in front sends more than one read's worth of headers.
    inline std::string readRequest(int client, std::chrono::milliseconds budget)
    {
        constexpr std::size_t MaxRequest = 64 * 1024;
        auto const deadline = Clock::now() + budget;
        auto request = std::string{};
        auto wanted = std::string::npos;
        char buffer[4096];
        while (request.size() < MaxRequest && ready(client, POLLIN, deadline))
        {
            auto const n = ::recv(client, buffer, sizeof(buffer), MSG_DONTWAIT);
            if (n <= 0)
            {
                break;
            }
            // The blank line may straddle two reads.
            auto const from = request.size() < 3 ? 0 : request.size() - 3;
            request.append(buffer, static_cast<std::size_t>(n));
            if (wanted == std::string::npos)
            {
                auto const end = request.find("\r\n\r\n", from);
                if (end == std::string::npos)
                {
                    continue;
                }
                // A length that is no number counts as none: the request is
                // answered from its headers, and a bad body is refused there.
                auto const length = header(std::string_view{request}.substr(0, end + 2), "content-length");
                auto bodySize = std::size_t{0};
                std::from_chars(length.data(), length.data() + length.size(), bodySize);
                wanted = end + 4 + std::min(bodySize, MaxRequest);
            }
            if (request.size() >= wanted)
            {
                break;
            }
        }
        return request;
    }

    // The path with every run of slashes made one, so a client joining an href
    // that ends in '/' with a path that starts with one still reaches it.
    inline std::string collapseSlashes(std::string_view path)
    {
        auto out = std::string{};
        for (auto const c : path)
        {
            if (c != '/' || out.empty() || out.back() != '/')
            {
                out.push_back(c);
            }
        }
        return out;
    }

    // The CORS headers of an NMOS API response, which a controller running in
    // a browser on another origin, such as a registry's web UI, needs to read
    // it; with preflight those answering the OPTIONS a browser sends before a
    // PATCH.
    inline std::string corsHeaders(bool preflight)
    {
        return preflight ? "Access-Control-Allow-Origin: *\r\nAccess-Control-Allow-Methods: GET, PATCH, OPTIONS\r\n"
                           "Access-Control-Allow-Headers: Content-Type, Accept\r\nAccess-Control-Max-Age: 86400\r\n"
                         : "Access-Control-Allow-Origin: *\r\n";
    }

    // A request's method, target without the query, and body.
    struct Request
    {
        std::string method;
        std::string path;
        std::string body;
    };

    inline Request parseRequest(std::string const& raw)
    {
        auto r = Request{};
        auto const line = raw.substr(0, raw.find("\r\n"));
        auto const sp1 = line.find(' ');
        auto const sp2 = sp1 == std::string::npos ? std::string::npos : line.find(' ', sp1 + 1);
        if (sp2 == std::string::npos)
        {
            return r;
        }
        r.method = line.substr(0, sp1);
        auto const target = line.substr(sp1 + 1, sp2 - sp1 - 1);
        r.path = target.substr(0, target.find('?'));
        if (auto const end = raw.find("\r\n\r\n"); end != std::string::npos)
        {
            r.body = raw.substr(end + 4);
        }
        return r;
    }

    // An HTTP/1.1 exchange with a plain-HTTP server such as an NMOS registry:
    // the status code and body, or code 0 when it could not be had.
    struct Response
    {
        int code = 0;
        std::string body;
    };

    inline Response request(std::string const& host, int port, std::string const& method, std::string const& path,
        std::string const& body, std::chrono::milliseconds budget)
    {
        auto hints = addrinfo{};
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        addrinfo* found = nullptr;
        if (::getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &found) != 0 || found == nullptr)
        {
            return {};
        }
        auto const deadline = Clock::now() + budget;
        auto fd = -1;
        for (auto* a = found; a != nullptr && fd < 0; a = a->ai_next)
        {
            fd = ::socket(a->ai_family, a->ai_socktype | SOCK_NONBLOCK, a->ai_protocol);
            if (fd < 0)
            {
                continue;
            }
            if (::connect(fd, a->ai_addr, a->ai_addrlen) != 0 && !(errno == EINPROGRESS && ready(fd, POLLOUT, deadline)))
            {
                ::close(fd);
                fd = -1;
                continue;
            }
            auto soErr = 0;
            auto len = socklen_t{sizeof(soErr)};
            if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &soErr, &len) != 0 || soErr != 0)
            {
                ::close(fd);
                fd = -1;
            }
        }
        ::freeaddrinfo(found);
        if (fd < 0)
        {
            return {};
        }
        auto const left = std::chrono::ceil<std::chrono::milliseconds>(deadline - Clock::now());
        auto message = method + " " + path + " HTTP/1.1\r\nHost: " + host + ":" + std::to_string(port) +
                       "\r\nContent-Type: application/json\r\nContent-Length: " + std::to_string(body.size()) +
                       "\r\nConnection: close\r\n\r\n" + body;
        auto response = std::string{};
        if (left.count() > 0 && sendAll(fd, message, left))
        {
            // Read to the end of the body the response states, not to the
            // close: a server that keeps the connection open would otherwise
            // hold every request for its whole budget.
            auto wanted = std::string::npos;
            char buffer[4096];
            while (response.size() < wanted && ready(fd, POLLIN, deadline))
            {
                auto const n = ::recv(fd, buffer, sizeof(buffer), MSG_DONTWAIT);
                if (n <= 0)
                {
                    break;
                }
                response.append(buffer, static_cast<std::size_t>(n));
                if (wanted == std::string::npos)
                {
                    if (auto const end = response.find("\r\n\r\n"); end != std::string::npos)
                    {
                        auto const length = header(std::string_view{response}.substr(0, end + 2), "content-length");
                        auto size = std::size_t{0};
                        if (std::from_chars(length.data(), length.data() + length.size(), size).ec == std::errc{} && !length.empty())
                        {
                            wanted = end + 4 + size;
                        }
                    }
                }
            }
        }
        ::close(fd);
        auto out = Response{};
        if (response.starts_with("HTTP/1.") && response.size() > 12)
        {
            out.code = std::atoi(response.c_str() + 9);
        }
        if (auto const end = response.find("\r\n\r\n"); end != std::string::npos)
        {
            out.body = response.substr(end + 4);
        }
        return out;
    }

}
