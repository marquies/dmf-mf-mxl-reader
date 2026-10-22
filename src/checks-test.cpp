// Tests of what mxl-reader decides about a flow and its grains, and of how it
// reads a request and writes a response. Built and run by the image build, so
// an image carrying a wrong classification is never published.

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

    constexpr char const* V210 = R"({"media_type": "video/v210", "frame_width": 1296, "frame_height": 720,
        "interlace_mode": "progressive", "grain_rate": {"numerator": 30000, "denominator": 1001}})";

    reader::Declared v210()
    {
        return reader::parseDefinition(V210).value();
    }

    mxlGrainInfo wholeGrain(std::uint64_t index, reader::Declared const& d)
    {
        auto grain = mxlGrainInfo{};
        grain.index = index;
        grain.totalSlices = static_cast<std::uint16_t>(d.height);
        grain.validSlices = grain.totalSlices;
        grain.grainSize = static_cast<std::uint32_t>(reader::v210RowBytes(d.width) * static_cast<std::uint64_t>(reader::v210Rows(d)));
        return grain;
    }

    // A v210 picture of mid grey: every sample code 512, legal.
    std::vector<std::uint8_t> greyPicture(reader::Declared const& d)
    {
        auto bytes = std::vector<std::uint8_t>(reader::v210RowBytes(d.width) * static_cast<std::uint64_t>(reader::v210Rows(d)));
        auto const word = std::uint32_t{512U | (512U << 10) | (512U << 20)};
        for (std::size_t i = 0; i + 4 <= bytes.size(); i += 4)
        {
            std::memcpy(bytes.data() + i, &word, 4);
        }
        return bytes;
    }

    void definitions()
    {
        auto const d = v210();
        check(d.mediaType == "video/v210" && d.width == 1296 && d.height == 720, "a v210 definition is read");
        check(d.rate == "30000/1001", "the grain rate is kept as a rational");
        check(!d.interlaced && reader::v210Rows(d) == 720, "a progressive grain carries every row");

        auto const field = reader::parseDefinition(R"({"media_type": "video/v210", "frame_width": 1920, "frame_height": 1080,
            "interlace_mode": "interlaced_tff"})");
        check(field && reader::v210Rows(*field) == 540, "an interlaced grain carries one field, half the rows");

        auto const audio = reader::parseDefinition(R"({"media_type": "audio/float32", "sample_rate": {"numerator": 48000},
            "channel_count": 2})");
        check(audio && audio->sampleRate == 48000 && audio->channels == 2, "an audio definition is read");
        check(!reader::parseDefinition("not json"), "an unreadable definition is no definition");
    }

    void expectations()
    {
        auto const d = v210();
        check(reader::mismatch(d, std::nullopt).empty(), "nothing expected, nothing wrong");
        auto e = reader::Expectation{};
        e.width = 1296;
        e.height = 720;
        e.rate = "30000/1001";
        check(reader::mismatch(d, e).empty(), "a flow as expected is not wrong");
        e.width = 1920;
        e.height = 1080;
        check(reader::mismatch(d, e) == "1296x720 where 1920x1080 was expected", "the mismatch names both geometries");

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

    void grains()
    {
        auto const d = v210();
        auto picture = greyPicture(d);
        auto where = std::string{};
        auto grain = wholeGrain(7, d);
        check(reader::classifyGrain(grain, 7, picture.data(), d, false, true, &where) == reader::GrainOutcome::Ok,
            "a whole, legal grain of the declared size is ok");

        check(reader::classifyGrain(grain, 8, picture.data(), d, false, false, &where) == reader::GrainOutcome::Missing,
            "a slot still holding an older cycle's grain is missing, not ok");

        auto invalid = grain;
        invalid.flags = MXL_GRAIN_FLAG_INVALID;
        invalid.validSlices = 0;
        check(reader::classifyGrain(invalid, 7, picture.data(), d, false, false, &where) == reader::GrainOutcome::Invalid,
            "a gap-fill grain counts once, as invalid, not also as incomplete");

        auto partial = grain;
        partial.validSlices = 100;
        check(reader::classifyGrain(partial, 7, picture.data(), d, false, false, &where) == reader::GrainOutcome::Incomplete,
            "a grain left part-written is incomplete");

        auto small = grain;
        small.grainSize -= 128;
        check(reader::classifyGrain(small, 7, picture.data(), d, false, false, &where) == reader::GrainOutcome::WrongSize,
            "a grain smaller than its geometry needs is the wrong size");

        check(reader::classifyGrain(small, 8, picture.data(), d, true, false, &where) == reader::GrainOutcome::WrongFormat,
            "a flow declaring the wrong format counts as that, whatever else is wrong");

        auto const zero = std::uint32_t{0};
        std::memcpy(picture.data() + reader::v210RowBytes(d.width) * 719 + 8, &zero, 4);
        check(reader::classifyGrain(grain, 7, picture.data(), d, false, false, &where) == reader::GrainOutcome::Ok,
            "sample values are not checked unless asked");
        check(reader::classifyGrain(grain, 7, picture.data(), d, false, true, &where) == reader::GrainOutcome::Illegal,
            "a reserved code in the last row is illegal when asked");
        check(where == "v210 code 0 at row 719, word 2, bit 0", "the illegal sample is located");
    }

    void samples()
    {
        auto left = std::vector<float>{0.5F, -0.25F, 1.0F};
        auto slices = mxlWrappedMultiBufferSlice{};
        slices.base.fragments[0].pointer = left.data();
        slices.base.fragments[0].size = left.size() * sizeof(float);
        slices.stride = 0;
        slices.count = 1;
        check(reader::samplesLegal(slices), "samples within -1..1 are legal");
        left[1] = 1.5F;
        check(!reader::samplesLegal(slices), "a sample beyond full scale is not");
        left[1] = std::nanf("");
        check(!reader::samplesLegal(slices), "a sample that is not a number is not");
    }

    void labels()
    {
        check(reader::labelValue("video/v210") == "video/v210", "a media type passes as a label");
        check(reader::labelValue("a\"b\\c\nd") == "a_b_c_d", "quotes, backslashes and newlines cannot break a label");
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
    definitions();
    expectations();
    grains();
    samples();
    labels();
    longRequest();
    stalledClients();
    if (failures == 0)
    {
        std::fprintf(stderr, "all checks pass\n");
    }
    return failures == 0 ? 0 : 1;
}
