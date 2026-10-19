// mxl-reader: keeps MXL flows open and reads every grain or sample batch as it
// lands, so the node it runs on holds a live consumer of each flow.
//
// A consumer is what makes mxl-k8s replicate a flow to a node, and reading at
// the head is what shows whether that replica keeps up. Each flow is read on a
// thread of its own and reported on an HTTP endpoint in the Prometheus text
// format, with /ready answering 200 only while every flow is delivering.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <CLI/CLI.hpp>
#include <fmt/format.h>
#include <mxl/flow.h>
#include <mxl/flowinfo.h>
#include <mxl/mxl.h>
#include <mxl/time.h>

namespace
{
    std::atomic<bool> stopping{false};

    void onSignal(int)
    {
        stopping = true;
    }

    constexpr std::uint64_t NsPerSecond = 1'000'000'000ULL;

    struct FlowStats
    {
        std::string id;
        std::atomic<bool> open{false};
        std::atomic<bool> continuous{false};
        std::atomic<std::uint64_t> read{0};
        std::atomic<std::uint64_t> invalid{0};
        std::atomic<std::uint64_t> skipped{0};
        std::atomic<std::uint64_t> stalls{0};
        std::atomic<std::uint64_t> opens{0};
        std::atomic<std::uint64_t> lastReadAt{0};
        std::atomic<std::int64_t> ageNs{0};
        std::atomic<int> lastStatus{MXL_STATUS_OK};
    };

    std::uint64_t periodNs(mxlRational const& rate)
    {
        if (rate.numerator <= 0 || rate.denominator <= 0)
        {
            return NsPerSecond / 50;
        }
        return NsPerSecond * static_cast<std::uint64_t>(rate.denominator) / static_cast<std::uint64_t>(rate.numerator);
    }

    std::uint64_t headIndex(mxlFlowReader reader, std::uint64_t fallback)
    {
        auto runtime = mxlFlowRuntimeInfo{};
        if (mxlFlowReaderGetRuntimeInfo(reader, &runtime) == MXL_STATUS_OK && runtime.headIndex != 0)
        {
            return runtime.headIndex;
        }
        return fallback;
    }

    void markRead(FlowStats& stats, mxlRational const& rate, std::uint64_t index, std::uint64_t units)
    {
        auto const now = mxlGetTime();
        stats.read += units;
        stats.lastReadAt = now;
        stats.ageNs = static_cast<std::int64_t>(now) - static_cast<std::int64_t>(mxlIndexToTimestamp(&rate, index));
    }

    // Nothing to read yet. Either call may return before its wait expires
    // (GetSamples answers too early as soon as the range is incomplete), so a
    // stall is counted once per timeout without progress rather than per call,
    // and the thread sleeps until the index is due instead of spinning.
    void awaitIndex(FlowStats& stats, mxlRational const& rate, std::uint64_t index, std::uint64_t timeoutNs, std::uint64_t& waitingSince)
    {
        auto const now = mxlGetTime();
        if (waitingSince == 0)
        {
            waitingSince = now;
        }
        else if (now - waitingSince >= timeoutNs)
        {
            ++stats.stalls;
            waitingSince = now;
        }
        mxlSleepForNs(std::clamp<std::uint64_t>(mxlGetNsUntilIndex(index, &rate), NsPerSecond / 1000, timeoutNs));
    }

    // Returns the status that ended the loop: anything other than a read that
    // is merely early or late means the reader no longer holds a usable flow.
    mxlStatus readDiscrete(mxlFlowReader reader, mxlRational const& rate, FlowStats& stats)
    {
        // Several grain periods, so a writer that commits a grain late is a
        // slow grain rather than a timeout, while a stopped writer still shows.
        auto const timeoutNs = std::max<std::uint64_t>(4 * periodNs(rate), NsPerSecond / 10);
        auto index = headIndex(reader, mxlGetCurrentIndex(&rate));
        std::uint64_t waitingSince = 0;
        while (!stopping)
        {
            auto grain = mxlGrainInfo{};
            std::uint8_t* payload = nullptr;
            auto const status = mxlFlowReaderGetGrain(reader, index, timeoutNs, &grain, &payload);
            switch (status)
            {
                case MXL_STATUS_OK:
                    if ((grain.flags & MXL_GRAIN_FLAG_INVALID) != 0)
                    {
                        ++stats.invalid;
                    }
                    markRead(stats, rate, index, 1);
                    ++index;
                    waitingSince = 0;
                    break;
                case MXL_ERR_OUT_OF_RANGE_TOO_LATE:
                {
                    // The ring overwrote the grain before it was read: jump to
                    // the head and count what was lost on the way.
                    auto const head = headIndex(reader, mxlGetCurrentIndex(&rate));
                    stats.skipped += head > index ? head - index : 1;
                    index = head > index ? head : index + 1;
                    break;
                }
                case MXL_ERR_OUT_OF_RANGE_TOO_EARLY:
                case MXL_ERR_TIMEOUT:               awaitIndex(stats, rate, index + 1, timeoutNs, waitingSince); break;
                default:                            return status;
            }
        }
        return MXL_STATUS_OK;
    }

    mxlStatus readContinuous(mxlFlowReader reader, mxlFlowConfigInfo const& config, FlowStats& stats)
    {
        auto const& rate = config.common.grainRate;
        // Ten milliseconds of samples, or the writer's own signalling batch when
        // it states one: asking for less than a batch only waits for the rest.
        auto batch = std::max<std::uint64_t>(1, static_cast<std::uint64_t>(rate.numerator / std::max<std::int64_t>(1, rate.denominator) / 100));
        if (config.common.maxSyncBatchSizeHint > 0)
        {
            batch = std::max<std::uint64_t>(batch, config.common.maxSyncBatchSizeHint);
        }
        auto const timeoutNs = std::max<std::uint64_t>(4 * batch * periodNs(rate), NsPerSecond / 10);
        auto index = headIndex(reader, mxlGetCurrentIndex(&rate));
        std::uint64_t waitingSince = 0;
        while (!stopping)
        {
            auto const next = index + batch;
            auto slices = mxlWrappedMultiBufferSlice{};
            auto const status = mxlFlowReaderGetSamples(reader, next, batch, timeoutNs, &slices);
            switch (status)
            {
                case MXL_STATUS_OK:
                    markRead(stats, rate, next, batch);
                    index = next;
                    waitingSince = 0;
                    break;
                case MXL_ERR_OUT_OF_RANGE_TOO_LATE:
                {
                    auto const head = headIndex(reader, mxlGetCurrentIndex(&rate));
                    stats.skipped += head > index ? head - index : batch;
                    index = head > index ? head : next;
                    break;
                }
                case MXL_ERR_OUT_OF_RANGE_TOO_EARLY:
                case MXL_ERR_TIMEOUT:               awaitIndex(stats, rate, next, timeoutNs, waitingSince); break;
                default:                            return status;
            }
        }
        return MXL_STATUS_OK;
    }

    void readFlow(std::string const& domain, FlowStats& stats)
    {
        auto* instance = mxlCreateInstance(domain.c_str(), "");
        if (instance == nullptr)
        {
            std::fprintf(stderr, "flow %s: cannot open MXL domain %s\n", stats.id.c_str(), domain.c_str());
            stopping = true;
            return;
        }

        while (!stopping)
        {
            mxlFlowReader reader = nullptr;
            auto status = mxlCreateFlowReader(instance, stats.id.c_str(), "", &reader);
            if (status != MXL_STATUS_OK)
            {
                // Not there yet: the flow is still being mirrored here, or its
                // writer has not started. Keep asking rather than exiting, so the
                // pod waits for the flow instead of crash-looping until it lands.
                if (stats.lastStatus.exchange(status) != status)
                {
                    std::fprintf(stderr, "flow %s: waiting, create reader returned %d\n", stats.id.c_str(), status);
                }
                mxlSleepForNs(NsPerSecond);
                continue;
            }

            auto info = mxlFlowInfo{};
            status = mxlFlowReaderGetInfo(reader, &info);
            if (status == MXL_STATUS_OK)
            {
                ++stats.opens;
                stats.open = true;
                stats.lastStatus = MXL_STATUS_OK;
                stats.continuous = info.config.common.format == MXL_DATA_FORMAT_AUDIO;
                std::fprintf(stderr, "flow %s: open, %s\n", stats.id.c_str(), stats.continuous ? "continuous" : "discrete");
                status = stats.continuous ? readContinuous(reader, info.config, stats)
                                          : readDiscrete(reader, info.config.common.grainRate, stats);
            }
            stats.open = false;
            mxlReleaseFlowReader(instance, reader);
            if (!stopping)
            {
                stats.lastStatus = status;
                std::fprintf(stderr, "flow %s: lost (status %d), reopening\n", stats.id.c_str(), status);
                mxlSleepForNs(NsPerSecond);
            }
        }
        mxlDestroyInstance(instance);
    }

    // Delivering means a read landed within the last second. A flow that is
    // open but silent is the failure this reader exists to show.
    bool delivering(FlowStats const& stats)
    {
        auto const last = stats.lastReadAt.load();
        return stats.open && last != 0 && mxlGetTime() - last < NsPerSecond;
    }

    std::string metrics(std::vector<std::unique_ptr<FlowStats>> const& flows)
    {
        struct Series
        {
            char const* name;
            char const* type;
            char const* help;
            std::uint64_t (*value)(FlowStats const&);
        };
        static Series const series[] = {
            {"mxl_reader_flow_open",        "gauge",   "1 while a reader holds the flow.",
             [](FlowStats const& s) -> std::uint64_t { return s.open ? 1 : 0; }},
            {"mxl_reader_flow_delivering",  "gauge",   "1 while reads land within a second.",
             [](FlowStats const& s) -> std::uint64_t { return delivering(s) ? 1 : 0; }},
            {"mxl_reader_read_total",       "counter", "Grains (discrete) or samples (continuous) read.",
             [](FlowStats const& s) -> std::uint64_t { return s.read; }},
            {"mxl_reader_invalid_total",    "counter", "Grains read with the invalid flag set.",
             [](FlowStats const& s) -> std::uint64_t { return s.invalid; }},
            {"mxl_reader_skipped_total",    "counter", "Grains or samples overwritten before they were read.",
             [](FlowStats const& s) -> std::uint64_t { return s.skipped; }},
            {"mxl_reader_stalls_total",     "counter", "Read timeouts that passed with nothing new to read.",
             [](FlowStats const& s) -> std::uint64_t { return s.stalls; }},
            {"mxl_reader_opens_total",      "counter", "Times the flow was opened.",
             [](FlowStats const& s) -> std::uint64_t { return s.opens; }},
        };

        std::string out;
        for (auto const& s : series)
        {
            out += fmt::format("# HELP {} {}\n# TYPE {} {}\n", s.name, s.help, s.name, s.type);
            for (auto const& flow : flows)
            {
                out += fmt::format("{}{{flow=\"{}\",unit=\"{}\"}} {}\n",
                    s.name, flow->id, flow->continuous ? "samples" : "grains", s.value(*flow));
            }
        }
        out += "# HELP mxl_reader_age_seconds Wall clock minus the media timestamp of the last read.\n"
               "# TYPE mxl_reader_age_seconds gauge\n";
        for (auto const& flow : flows)
        {
            out += fmt::format("mxl_reader_age_seconds{{flow=\"{}\"}} {:.6f}\n", flow->id, flow->ageNs / 1e9);
        }
        return out;
    }

    void serve(int port, std::vector<std::unique_ptr<FlowStats>> const& flows)
    {
        auto const fd = ::socket(AF_INET, SOCK_STREAM, 0);
        auto const one = 1;
        ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        auto addr = sockaddr_in{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        addr.sin_port = htons(static_cast<std::uint16_t>(port));
        if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 || ::listen(fd, 16) != 0)
        {
            std::fprintf(stderr, "cannot listen on :%d\n", port);
            stopping = true;
            return;
        }
        // accept() is not woken by a flag; a timeout lets the loop see stopping.
        auto const tv = timeval{.tv_sec = 1, .tv_usec = 0};
        ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        while (!stopping)
        {
            auto const client = ::accept(fd, nullptr, nullptr);
            if (client < 0)
            {
                continue;
            }
            ::setsockopt(client, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
            char request[1024] = {};
            auto const n = ::recv(client, request, sizeof(request) - 1, 0);
            auto const path = std::string{n > 0 ? request : ""};

            auto code = 200;
            std::string body;
            if (path.starts_with("GET /metrics"))
            {
                body = metrics(flows);
            }
            else if (path.starts_with("GET /ready"))
            {
                auto all = true;
                for (auto const& flow : flows)
                {
                    all = all && delivering(*flow);
                }
                code = all ? 200 : 503;
                body = all ? "delivering\n" : "not every flow is delivering\n";
            }
            else if (path.starts_with("GET /healthz"))
            {
                body = "ok\n";
            }
            else
            {
                code = 404;
                body = "not found\n";
            }
            auto const response = fmt::format("HTTP/1.1 {} {}\r\nContent-Type: text/plain; version=0.0.4\r\n"
                                              "Content-Length: {}\r\nConnection: close\r\n\r\n{}",
                code, code == 200 ? "OK" : code == 503 ? "Service Unavailable" : "Not Found", body.size(), body);
            ::send(client, response.data(), response.size(), MSG_NOSIGNAL);
            ::close(client);
        }
        ::close(fd);
    }
}

int main(int argc, char** argv)
{
    auto app = CLI::App{"mxl-reader"};
    auto domain = std::string{};
    app.add_option("-d,--domain", domain, "The MXL domain directory.")->required()->check(CLI::ExistingDirectory);
    auto flowIds = std::vector<std::string>{};
    app.add_option("-f,--flow", flowIds, "A flow id to read. Repeat for several.")->required();
    auto port = 9102;
    app.add_option("-p,--port", port, "Port of the metrics and readiness endpoint.");
    auto reportSeconds = 10;
    app.add_option("--report-interval", reportSeconds, "Seconds between summary lines on stderr. 0 disables them.");
    CLI11_PARSE(app, argc, argv);

    std::signal(SIGTERM, onSignal);
    std::signal(SIGINT, onSignal);

    auto flows = std::vector<std::unique_ptr<FlowStats>>{};
    for (auto const& id : flowIds)
    {
        flows.push_back(std::make_unique<FlowStats>());
        flows.back()->id = id;
    }

    auto threads = std::vector<std::thread>{};
    for (auto& flow : flows)
    {
        threads.emplace_back(readFlow, domain, std::ref(*flow));
    }
    threads.emplace_back(serve, port, std::cref(flows));

    auto previous = std::vector<std::uint64_t>(flows.size(), 0);
    auto waited = 0;
    while (!stopping)
    {
        mxlSleepForNs(NsPerSecond);
        if (reportSeconds <= 0 || ++waited < reportSeconds)
        {
            continue;
        }
        waited = 0;
        for (std::size_t i = 0; i < flows.size(); ++i)
        {
            auto const& flow = *flows[i];
            auto const read = flow.read.load();
            std::fprintf(stderr, "flow %s: %s, %.1f %s/s, skipped %llu, invalid %llu, stalls %llu, age %.3f s\n",
                flow.id.c_str(), delivering(flow) ? "delivering" : flow.open ? "silent" : "closed",
                static_cast<double>(read - previous[i]) / reportSeconds, flow.continuous ? "samples" : "grains",
                static_cast<unsigned long long>(flow.skipped.load()), static_cast<unsigned long long>(flow.invalid.load()),
                static_cast<unsigned long long>(flow.stalls.load()), flow.ageNs / 1e9);
            previous[i] = read;
        }
    }

    for (auto& thread : threads)
    {
        thread.join();
    }
    return 0;
}
