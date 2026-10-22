// Reading a request and writing a response on mxl-reader's HTTP socket. Kept
// apart so it can be tested over a real connection.
//
// Both are bounded in time as a whole, not per call, because requests are
// answered one at a time: a client that trickles bytes in or reads the answer
// slowly must not hold the probes queued behind it.

#pragma once

#include <poll.h>
#include <sys/socket.h>

#include <chrono>
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

    // The request up to the end of its headers. Closing a socket with unread
    // bytes in it resets the connection, and a reset drops whatever of the
    // response is still unsent; a proxy in front sends more than one read's
    // worth of headers.
    inline std::string readRequest(int client, std::chrono::milliseconds budget)
    {
        constexpr std::size_t MaxRequest = 64 * 1024;
        auto const deadline = Clock::now() + budget;
        auto request = std::string{};
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
            if (request.find("\r\n\r\n", from) != std::string::npos)
            {
                break;
            }
        }
        return request;
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
}
