// Tests of what mxl-reader is told to expect of a flow, and of how it reads a
// request and writes a response. Built and run by the image build, so an
// image that misreads --expect is never published. The checks a grain or a
// batch of samples goes through are libmxl-monitoring's, tested there.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "checks.hpp"
#include "http.hpp"

namespace
{
    int failures = 0;

    void check(bool ok, char const* what)
    {
        if (!ok)
        {
            std::fprintf(stderr, "FAIL: %s\n", what);
            ++failures;
        }
    }

    void expectations()
    {
        auto parsed = std::map<std::string, reader::Expectation>{};
        auto const id = std::string{"d4d00000-0000-0000-0000-000000000001"};
        check(reader::parseExpectations(R"({"d4d00000-0000-0000-0000-000000000001": {"width": 1920, "checkLegal": true}})", {id}, parsed)
                  .empty(),
            "a usable --expect is taken");
        check(parsed[id].width == 1920 && parsed[id].checkLegal, "its fields reach the expectation");
        parsed.clear();
        check(!reader::parseExpectations(R"({"d4d00000-0000-0000-0000-000000000001": {"samplerate": 48000}})", {id}, parsed).empty(),
            "a misspelt key is refused rather than checking nothing");
        check(!reader::parseExpectations(R"({"D4D00000-0000-0000-0000-000000000001": {}})", {id}, parsed).empty(),
            "an expectation for a flow not read is refused");
    }

    // What --expect says reaches the library intact, so its checks compare a
    // flow with what was asked for.
    void conversion()
    {
        auto e = reader::Expectation{};
        e.mediaType = "video/v210";
        e.width = 1920;
        e.height = 1080;
        e.rate = "30000/1001";
        e.checkLegal = true;
        auto const m = reader::toMonitoring(e);
        check(m.version == 1 && m.size == sizeof(m), "the expectation is versioned for the library");
        check(m.mediaType == e.mediaType, "the media type is passed on");
        check(m.width == 1920 && m.height == 1080 && m.checkLegal, "geometry and the legal check are passed on");
        check(m.rate.numerator == 30000 && m.rate.denominator == 1001, "the rate is passed on as a rational");
        check(reader::toMonitoring(reader::Expectation{}).mediaType == nullptr, "an unset media type is not checked");

        auto declared = mxlMonitoringDeclared{};
        mxlMonitoringParseDefinition(R"({"media_type": "video/v210", "frame_width": 1296, "frame_height": 720,
            "grain_rate": {"numerator": 30000, "denominator": 1001}})",
            &declared);
        char message[MXL_MONITORING_TEXT];
        check(mxlMonitoringMismatch(&declared, &m, message, sizeof(message)) == (1U << MXL_MONITORING_CONTENT_WRONG_RESOLUTION),
            "a flow of another size differs in resolution only");
        check(std::string{message} == "1296x720 where 1920x1080 was expected", "the mismatch names both geometries");
    }

    // A connected loopback pair, both ends with a receive timeout, so a
    // regression fails a check rather than hanging the build.
    struct Pair
    {
        int server = -1;
        int client = -1;
    };

    Pair connected()
    {
        auto const listener = ::socket(AF_INET, SOCK_STREAM, 0);
        auto addr = sockaddr_in{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        auto len = socklen_t{sizeof(addr)};
        auto const tv = timeval{.tv_sec = 5, .tv_usec = 0};
        auto pair = Pair{};
        if (listener >= 0 && ::bind(listener, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0 && ::listen(listener, 1) == 0
            && ::getsockname(listener, reinterpret_cast<sockaddr*>(&addr), &len) == 0)
        {
            pair.client = ::socket(AF_INET, SOCK_STREAM, 0);
            if (pair.client >= 0 && ::connect(pair.client, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0)
            {
                pair.server = ::accept(listener, nullptr, nullptr);
            }
        }
        if (listener >= 0)
        {
            ::close(listener);
        }
        check(pair.server >= 0 && pair.client >= 0, "a loopback connection opens");
        ::setsockopt(pair.server, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        ::setsockopt(pair.client, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        return pair;
    }

    // A request longer than one read, in two segments, as a proxy sends one,
    // answered with more than the connection carries before its first
    // acknowledgement: the whole answer has to arrive, not a reset after the
    // first part of it.
    void longRequest()
    {
        auto const pair = connected();
        if (pair.server < 0 || pair.client < 0)
        {
            return;
        }
        auto const head = "GET /page HTTP/1.1\r\nX-Padding: " + std::string(3000, 'x');
        auto const tail = std::string{"\r\nHost: reader\r\n\r\n"};
        auto const body = std::string(4 * 1024 * 1024, 'y');
        auto received = std::string{};
        auto sent = false;
        auto server = std::thread{[&] {
            received = reader::readRequest(pair.server, std::chrono::seconds{2});
            sent = reader::sendAll(pair.server, body, std::chrono::seconds{2});
            ::close(pair.server);
        }};

        ::send(pair.client, head.data(), head.size(), MSG_NOSIGNAL);
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        ::send(pair.client, tail.data(), tail.size(), MSG_NOSIGNAL);
        auto got = std::size_t{0};
        auto reset = false;
        char buffer[65536];
        for (;;)
        {
            auto const n = ::recv(pair.client, buffer, sizeof(buffer), 0);
            if (n < 0)
            {
                reset = errno == ECONNRESET;
                break;
            }
            if (n == 0)
            {
                break;
            }
            got += static_cast<std::size_t>(n);
        }
        server.join();
        ::close(pair.client);

        check(received == head + tail, "a request is read through the end of its headers, across segments");
        check(sent, "a response longer than one send goes out whole");
        check(!reset, "answering a long request does not reset the connection");
        check(got == body.size(), "the whole response arrives");
    }

    // A client that stops halfway through its headers, or stops reading the
    // answer, holds the serve loop for its budget and no longer.
    void stalledClients()
    {
        auto const pair = connected();
        if (pair.server < 0 || pair.client < 0)
        {
            return;
        }
        auto const line = std::string{"GET /status HTTP/1.1\r\n"};
        ::send(pair.client, line.data(), line.size(), MSG_NOSIGNAL);
        auto start = reader::Clock::now();
        auto const partial = reader::readRequest(pair.server, std::chrono::milliseconds{100});
        auto const readFor = reader::Clock::now() - start;
        check(partial == line, "a request that stops halfway is returned as far as it came");
        check(readFor >= std::chrono::milliseconds{90} && readFor < std::chrono::seconds{1}, "a stalled request is given up after its budget");

        // Closing the reading end after a second fails a send that ignores
        // its budget, instead of leaving it blocked.
        auto watchdog = std::thread{[&] {
            std::this_thread::sleep_for(std::chrono::seconds{1});
            ::close(pair.client);
        }};
        start = reader::Clock::now();
        auto const sent = reader::sendAll(pair.server, std::string(64 * 1024 * 1024, 'y'), std::chrono::milliseconds{100});
        auto const sentFor = reader::Clock::now() - start;
        watchdog.join();
        check(!sent, "an answer nobody reads is not reported as sent");
        check(sentFor < std::chrono::milliseconds{900}, "an answer nobody reads is given up after its budget");
        ::close(pair.server);
    }
}

int main()
{
    expectations();
    conversion();
    longRequest();
    stalledClients();
    if (failures == 0)
    {
        std::fprintf(stderr, "all checks pass\n");
    }
    return failures == 0 ? 0 : 1;
}
