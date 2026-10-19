// mxl-reader: keeps MXL flows open and reads every grain or sample batch as it
// lands, so the node it runs on holds a live consumer of each flow.
//
// A consumer is what makes mxl-k8s replicate a flow to a node, and reading at
// the head is what shows whether that replica keeps up. Each flow is read on a
// thread of its own. Every read is classified - correct, not there when due,
// overtaken, unreadable, or there but wrong - and counted, so a load or failure
// test sees exactly which way a flow went bad. The counts are served as
// Prometheus text on /metrics and as JSON on /status; /delivering answers 200
// only while every flow delivers.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cmath>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <regex>
#include <string>
#include <thread>
#include <vector>

#include <CLI/CLI.hpp>
#include <fmt/format.h>
#include <mxl/flow.h>
#include <mxl/flowinfo.h>
#include <mxl/mxl.h>
#include <mxl/time.h>
#include <picojson/wrapper.h>

namespace
{
    std::atomic<bool> stopping{false};
    // Set by a thread that cannot do its job at all, so the process exits
    // non-zero instead of looking like a clean shutdown.
    std::atomic<bool> failed{false};

    void onSignal(int)
    {
        stopping = true;
    }

    constexpr std::uint64_t NsPerSecond = 1'000'000'000ULL;

    // What a reader was told to expect of a flow. Unset fields are not checked.
    struct Expectation
    {
        std::string mediaType;
        int width = 0;
        int height = 0;
        std::string rate;
        int sampleRate = 0;
        int channels = 0;
        // Check every sample against the legal range. Off unless asked: a
        // test pattern may carry reserved codes on purpose, SMPTE bars write
        // super-black as luma code 0, and a check that always fires hides the
        // faults it is there to find.
        bool checkLegal = false;
    };

    // Where a flow's reader stands, as one word for a page and a metric label.
    enum class State
    {
        Starting,
        DomainUnavailable,
        FlowNotFound,
        PermissionDenied,
        FlowError,
        Open,
    };

    char const* stateName(State state)
    {
        switch (state)
        {
            case State::Starting:          return "starting";
            case State::DomainUnavailable: return "domain_unavailable";
            case State::FlowNotFound:      return "flow_not_found";
            case State::PermissionDenied:  return "permission_denied";
            case State::FlowError:         return "flow_error";
            case State::Open:              return "open";
        }
        return "unknown";
    }

    struct FlowStats
    {
        std::string id;
        std::optional<Expectation> expected;

        std::atomic<State> state{State::Starting};
        std::atomic<bool> continuous{false};

        // Reads, in grains (discrete) or samples (continuous); tooEarly and
        // tooLate count events.
        std::atomic<std::uint64_t> ok{0};
        std::atomic<std::uint64_t> tooEarly{0};
        std::atomic<std::uint64_t> tooLate{0};
        std::atomic<std::uint64_t> skipped{0};
        std::atomic<std::uint64_t> invalid{0};
        std::atomic<std::uint64_t> incomplete{0};
        std::atomic<std::uint64_t> wrongSize{0};
        std::atomic<std::uint64_t> wrongFormat{0};
        std::atomic<std::uint64_t> illegal{0};
        std::atomic<std::uint64_t> readPermission{0};
        std::atomic<std::uint64_t> readOther{0};

        // Attempts to reach the flow at all.
        std::atomic<std::uint64_t> domainErrors{0};
        std::atomic<std::uint64_t> flowNotFound{0};
        std::atomic<std::uint64_t> flowPermission{0};
        std::atomic<std::uint64_t> flowOther{0};
        std::atomic<std::uint64_t> opens{0};
        std::atomic<std::uint64_t> lost{0};
        std::atomic<std::uint64_t> stalls{0};

        std::atomic<std::uint64_t> lastReadAt{0};
        std::atomic<std::uint64_t> lastProgressAt{0};
        std::atomic<bool> stallCounted{false};
        std::atomic<std::int64_t> ageNs{0};

        // Read by the HTTP thread, written by the flow's own.
        mutable std::mutex mutex;
        std::string mediaType;
        std::string format;
        std::string formatMismatch;
        std::string lastError;
        std::uint64_t lastErrorAt = 0;

        void error(std::string message)
        {
            auto const lock = std::lock_guard{mutex};
            if (message != lastError)
            {
                std::fprintf(stderr, "flow %s: %s\n", id.c_str(), message.c_str());
            }
            lastError = std::move(message);
            lastErrorAt = mxlGetTime();
        }
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

    void markRead(FlowStats& stats, mxlRational const& rate, std::uint64_t index)
    {
        auto const now = mxlGetTime();
        stats.lastReadAt = now;
        stats.lastProgressAt = now;
        stats.stallCounted = false;
        stats.ageNs = static_cast<std::int64_t>(now) - static_cast<std::int64_t>(mxlIndexToTimestamp(&rate, index));
    }

    // Nothing to read yet. That is the normal state of a reader at the head
    // until the next grain is due; it counts as too early only once the index
    // is a grace period past due, and once per index. A stall is a whole read
    // timeout with no read at all, counted once until reads resume. The sleep
    // lasts until the index is due, at most 5 ms, so a non-blocking read cannot
    // spin. Returns whether a whole timeout has passed without a read.
    bool awaitIndex(FlowStats& stats, mxlRational const& rate, std::uint64_t index, std::uint64_t graceNs,
        std::uint64_t timeoutNs, std::uint64_t& earlyCounted)
    {
        auto const now = mxlGetTime();
        if (earlyCounted != index && now > mxlIndexToTimestamp(&rate, index) + graceNs)
        {
            ++stats.tooEarly;
            earlyCounted = index;
        }
        auto const timedOut = now - stats.lastProgressAt >= timeoutNs;
        if (timedOut && !stats.stallCounted.exchange(true))
        {
            ++stats.stalls;
        }
        mxlSleepForNs(std::clamp<std::uint64_t>(mxlGetNsUntilIndex(index, &rate), NsPerSecond / 1000, NsPerSecond / 200));
        return timedOut;
    }

    // How long past its timestamp data may land before it counts as too
    // early: the span one read covers, or for samples the writer's commit
    // batch if larger, plus 50 ms for scheduling. A discrete flow's commit
    // hint counts slices within one grain, so it adds nothing there.
    std::uint64_t graceNs(mxlFlowConfigInfo const& config, std::uint64_t readNs, bool continuous)
    {
        auto const commitNs = continuous ? config.common.maxCommitBatchSizeHint * periodNs(config.common.grainRate) : 0;
        return std::max(readNs, commitNs) + NsPerSecond / 20;
    }

    // A v210 row packs six pixels into four 32-bit words of three 10-bit
    // samples, in 128-byte groups of 48 pixels.
    std::uint64_t v210RowBytes(int width)
    {
        return static_cast<std::uint64_t>((width + 47) / 48) * 128;
    }

    // v210 reserves the codes 0-3 and 1020-1023 for timing references, so a
    // picture sample there is not a legal value. Three rows are checked per
    // grain, the active pixels only: a row padded out to 48 pixels carries
    // zeros past the picture.
    // Where the first illegal sample sits, or "" when the rows are legal.
    std::string v210RowsIllegal(std::uint8_t const* payload, int width, int height)
    {
        auto const rowBytes = v210RowBytes(width);
        auto const activeWords = static_cast<std::uint64_t>(width / 6) * 4;
        for (auto const row : {0, height / 2, height - 1})
        {
            auto const* words = reinterpret_cast<std::uint32_t const*>(payload + rowBytes * static_cast<std::uint64_t>(row));
            for (std::uint64_t w = 0; w < activeWords; ++w)
            {
                auto const word = words[w];
                for (auto const shift : {0U, 10U, 20U})
                {
                    auto const sample = (word >> shift) & 0x3ffU;
                    if (sample < 4 || sample > 1019)
                    {
                        return fmt::format("v210 code {} at row {}, word {}, bit {}", sample, row, w, shift);
                    }
                }
            }
        }
        return {};
    }

    // What a flow declares about itself, read from its flow definition.
    struct Declared
    {
        std::string mediaType;
        int width = 0;
        int height = 0;
        std::string rate;
        int sampleRate = 0;
        int channels = 0;
    };

    int jsonInt(picojson::object const& object, char const* key)
    {
        auto const it = object.find(key);
        return (it != object.end() && it->second.is<double>()) ? static_cast<int>(it->second.get<double>()) : 0;
    }

    std::optional<Declared> readDefinition(mxlInstance instance, std::string const& id)
    {
        auto buffer = std::vector<char>(8192);
        auto size = buffer.size();
        auto status = mxlGetFlowDef(instance, id.c_str(), buffer.data(), &size);
        if (status == MXL_ERR_INVALID_ARG && size > buffer.size())
        {
            buffer.resize(size);
            status = mxlGetFlowDef(instance, id.c_str(), buffer.data(), &size);
        }
        if (status != MXL_STATUS_OK)
        {
            return std::nullopt;
        }
        auto json = picojson::value{};
        if (!picojson::parse(json, buffer.data()).empty() || !json.is<picojson::object>())
        {
            return std::nullopt;
        }
        auto const& object = json.get<picojson::object>();
        auto declared = Declared{};
        if (auto const it = object.find("media_type"); it != object.end() && it->second.is<std::string>())
        {
            declared.mediaType = it->second.get<std::string>();
        }
        declared.width = jsonInt(object, "frame_width");
        declared.height = jsonInt(object, "frame_height");
        declared.channels = jsonInt(object, "channel_count");
        if (auto const it = object.find("grain_rate"); it != object.end() && it->second.is<picojson::object>())
        {
            auto const& r = it->second.get<picojson::object>();
            auto const den = jsonInt(r, "denominator");
            declared.rate = fmt::format("{}/{}", jsonInt(r, "numerator"), den > 0 ? den : 1);
        }
        if (auto const it = object.find("sample_rate"); it != object.end() && it->second.is<picojson::object>())
        {
            declared.sampleRate = jsonInt(it->second.get<picojson::object>(), "numerator");
        }
        return declared;
    }

    // How the flow differs from what was expected, or "" when it does not.
    std::string mismatch(Declared const& declared, std::optional<Expectation> const& expected)
    {
        if (!expected)
        {
            return {};
        }
        auto out = std::vector<std::string>{};
        auto const& e = *expected;
        if (!e.mediaType.empty() && e.mediaType != declared.mediaType)
        {
            out.push_back(fmt::format("media type {} where {} was expected", declared.mediaType, e.mediaType));
        }
        if ((e.width && e.width != declared.width) || (e.height && e.height != declared.height))
        {
            out.push_back(fmt::format("{}x{} where {}x{} was expected", declared.width, declared.height, e.width, e.height));
        }
        if (!e.rate.empty() && e.rate != declared.rate)
        {
            out.push_back(fmt::format("rate {} where {} was expected", declared.rate, e.rate));
        }
        if (e.sampleRate && e.sampleRate != declared.sampleRate)
        {
            out.push_back(fmt::format("sample rate {} where {} was expected", declared.sampleRate, e.sampleRate));
        }
        if (e.channels && e.channels != declared.channels)
        {
            out.push_back(fmt::format("{} channels where {} were expected", declared.channels, e.channels));
        }
        auto joined = std::string{};
        for (auto const& part : out)
        {
            joined += (joined.empty() ? "" : "; ") + part;
        }
        return joined;
    }

    // libmxl reports a reader it could not open for want of permission as an
    // unknown error, so the files themselves are asked.
    bool permissionDenied(std::string const& domain, std::string const& id)
    {
        auto const dir = fmt::format("{}/{}.mxl-flow", domain, id);
        for (auto const& path : {dir, dir + "/flow_def.json", dir + "/data"})
        {
            struct stat st{};
            if (::stat(path.c_str(), &st) == 0 && ::access(path.c_str(), R_OK) != 0 && (errno == EACCES || errno == EPERM))
            {
                return true;
            }
        }
        return false;
    }

    // Returns the status that ended the loop: anything other than a read that
    // is merely early or late means the reader no longer holds a usable flow.
    mxlStatus readDiscrete(mxlFlowReader reader, mxlFlowConfigInfo const& config, Declared const& declared, bool wrongFormat,
        FlowStats& stats)
    {
        auto const& rate = config.common.grainRate;
        // Several grain periods, so a writer that commits a grain late is a
        // slow grain rather than a stall, while a stopped writer still shows.
        auto const period = periodNs(rate);
        auto const timeoutNs = std::max<std::uint64_t>(4 * period, NsPerSecond / 10);
        auto const grace = graceNs(config, period, false);
        auto const checkLegal = stats.expected && stats.expected->checkLegal;
        auto const v210 = declared.mediaType == "video/v210" && declared.width > 0 && declared.height > 0;
        auto const expectedSize = v210 ? v210RowBytes(declared.width) * static_cast<std::uint64_t>(declared.height) : 0;
        auto index = headIndex(reader, mxlGetCurrentIndex(&rate));
        auto earlyCounted = std::uint64_t{0};
        while (!stopping)
        {
            auto grain = mxlGrainInfo{};
            std::uint8_t* payload = nullptr;
            // Non-blocking, as for samples, and whole grains only: a blocking
            // read sleeps to its deadline whenever a writer signals less often
            // than it commits.
            auto const status = mxlFlowReaderGetGrainSliceNonBlocking(reader, index, MXL_GRAIN_VALID_SLICES_ALL, &grain, &payload);
            switch (status)
            {
                case MXL_STATUS_OK:
                {
                    auto clean = !wrongFormat;
                    if (wrongFormat)
                    {
                        ++stats.wrongFormat;
                    }
                    if ((grain.flags & MXL_GRAIN_FLAG_INVALID) != 0)
                    {
                        ++stats.invalid;
                        clean = false;
                    }
                    if (grain.validSlices < grain.totalSlices)
                    {
                        ++stats.incomplete;
                        clean = false;
                    }
                    if (v210 && grain.grainSize != expectedSize)
                    {
                        ++stats.wrongSize;
                        clean = false;
                    }
                    else if (v210 && checkLegal && payload != nullptr)
                    {
                        if (auto const where = v210RowsIllegal(payload, declared.width, declared.height); !where.empty())
                        {
                            ++stats.illegal;
                            clean = false;
                            stats.error("illegal sample value: " + where);
                        }
                    }
                    if (clean)
                    {
                        ++stats.ok;
                    }
                    markRead(stats, rate, index);
                    ++index;
                    break;
                }
                case MXL_ERR_OUT_OF_RANGE_TOO_LATE:
                {
                    // The ring overwrote the grain before it was read: jump to
                    // the head and count what was lost on the way.
                    ++stats.tooLate;
                    auto const head = headIndex(reader, mxlGetCurrentIndex(&rate));
                    stats.skipped += head > index ? head - index : 1;
                    index = head > index ? head : index + 1;
                    break;
                }
                case MXL_ERR_OUT_OF_RANGE_TOO_EARLY:
                case MXL_ERR_TIMEOUT:
                    // A writer whose clock stepped back leaves its head behind
                    // the index waited on; follow the head once a whole timeout
                    // has passed instead of waiting for the clock to catch up.
                    if (awaitIndex(stats, rate, index, grace, timeoutNs, earlyCounted))
                    {
                        if (auto const head = headIndex(reader, index); head + 1 < index)
                        {
                            index = head + 1;
                        }
                    }
                    break;
                default:                            return status;
            }
        }
        return MXL_STATUS_OK;
    }

    bool samplesLegal(mxlWrappedMultiBufferSlice const& slices)
    {
        for (std::size_t channel = 0; channel < slices.count; ++channel)
        {
            for (auto const& fragment : slices.base.fragments)
            {
                if (fragment.pointer == nullptr)
                {
                    continue;
                }
                auto const* base = static_cast<std::uint8_t const*>(fragment.pointer) + channel * slices.stride;
                auto const* samples = reinterpret_cast<float const*>(base);
                for (std::size_t i = 0; i < fragment.size / sizeof(float); ++i)
                {
                    if (!std::isfinite(samples[i]) || std::fabs(samples[i]) > 1.0F)
                    {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    mxlStatus readContinuous(mxlFlowReader reader, mxlFlowConfigInfo const& config, Declared const& declared,
        bool wrongFormat, FlowStats& stats)
    {
        auto const& rate = config.common.grainRate;
        // Ten milliseconds of samples, or the writer's own signalling batch when
        // it states one: asking for less than a batch only waits for the rest.
        auto batch = std::max<std::uint64_t>(1, static_cast<std::uint64_t>(rate.numerator / std::max<std::int64_t>(1, rate.denominator) / 100));
        if (config.common.maxSyncBatchSizeHint > 0)
        {
            batch = std::max<std::uint64_t>(batch, config.common.maxSyncBatchSizeHint);
        }
        auto const batchNs = batch * periodNs(rate);
        auto const timeoutNs = std::max<std::uint64_t>(4 * batchNs, NsPerSecond / 10);
        auto const grace = graceNs(config, batchNs, true);
        auto const float32 = declared.mediaType == "audio/float32" && stats.expected && stats.expected->checkLegal;
        auto index = headIndex(reader, mxlGetCurrentIndex(&rate));
        auto earlyCounted = std::uint64_t{0};
        while (!stopping)
        {
            auto const next = index + batch;
            auto slices = mxlWrappedMultiBufferSlice{};
            // Non-blocking: a continuous writer moves the head on every commit
            // but wakes waiting readers only once a whole sync batch is done, so
            // a blocking read sleeps to its deadline over samples already there.
            auto const status = mxlFlowReaderGetSamplesNonBlocking(reader, next, batch, &slices);
            switch (status)
            {
                case MXL_STATUS_OK:
                    if (wrongFormat)
                    {
                        stats.wrongFormat += batch;
                    }
                    else if (float32 && !samplesLegal(slices))
                    {
                        stats.illegal += batch;
                    }
                    else
                    {
                        stats.ok += batch;
                    }
                    markRead(stats, rate, next);
                    index = next;
                    break;
                case MXL_ERR_OUT_OF_RANGE_TOO_LATE:
                {
                    ++stats.tooLate;
                    auto const head = headIndex(reader, mxlGetCurrentIndex(&rate));
                    stats.skipped += head > index ? head - index : batch;
                    index = head > index ? head : next;
                    break;
                }
                case MXL_ERR_OUT_OF_RANGE_TOO_EARLY:
                case MXL_ERR_TIMEOUT:
                    if (awaitIndex(stats, rate, next, grace, timeoutNs, earlyCounted))
                    {
                        if (auto const head = headIndex(reader, index); head < index)
                        {
                            index = head;
                        }
                    }
                    break;
                default:                            return status;
            }
        }
        return MXL_STATUS_OK;
    }

    void readFlow(std::string const& domain, FlowStats& stats)
    {
        mxlInstance instance = nullptr;
        while (!stopping)
        {
            if (instance == nullptr)
            {
                instance = mxlCreateInstance(domain.c_str(), "");
                if (instance == nullptr)
                {
                    // The volume may not be mounted yet, or the agent has not
                    // created the domain: wait for it rather than exit.
                    ++stats.domainErrors;
                    stats.state = State::DomainUnavailable;
                    struct stat st{};
                    stats.error(::stat(domain.c_str(), &st) == 0 ? fmt::format("cannot open MXL domain {}", domain)
                                                                  : fmt::format("MXL domain {} does not exist", domain));
                    mxlSleepForNs(NsPerSecond);
                    continue;
                }
            }

            mxlFlowReader reader = nullptr;
            auto status = mxlCreateFlowReader(instance, stats.id.c_str(), "", &reader);
            if (status != MXL_STATUS_OK)
            {
                // Not there yet: the flow is still being mirrored here, or its
                // writer has not started. Keep asking rather than exiting, so the
                // pod waits for the flow instead of crash-looping until it lands.
                if (status == MXL_ERR_FLOW_NOT_FOUND || status == MXL_ERR_NOT_FOUND)
                {
                    ++stats.flowNotFound;
                    stats.state = State::FlowNotFound;
                    stats.error("flow not found in the domain");
                }
                else if (status == MXL_ERR_PERMISSION_DENIED || permissionDenied(domain, stats.id))
                {
                    ++stats.flowPermission;
                    stats.state = State::PermissionDenied;
                    stats.error("permission denied opening the flow");
                }
                else
                {
                    ++stats.flowOther;
                    stats.state = State::FlowError;
                    stats.error(fmt::format("cannot open the flow: status {}", static_cast<int>(status)));
                }
                mxlSleepForNs(NsPerSecond);
                continue;
            }

            auto info = mxlFlowInfo{};
            status = mxlFlowReaderGetInfo(reader, &info);
            if (status == MXL_STATUS_OK)
            {
                auto const declared = readDefinition(instance, stats.id).value_or(Declared{});
                auto const wrong = mismatch(declared, stats.expected);
                {
                    auto const lock = std::lock_guard{stats.mutex};
                    stats.mediaType = declared.mediaType;
                    stats.format = declared.width ? fmt::format("{}x{} @ {}", declared.width, declared.height, declared.rate)
                                 : declared.sampleRate ? fmt::format("{} ch @ {}", declared.channels, declared.sampleRate)
                                                       : std::string{};
                    stats.formatMismatch = wrong;
                }
                if (!wrong.empty())
                {
                    stats.error("format differs from what was expected: " + wrong);
                }
                ++stats.opens;
                stats.continuous = info.config.common.format == MXL_DATA_FORMAT_AUDIO;
                stats.lastProgressAt = mxlGetTime();
                stats.state = State::Open;
                std::fprintf(stderr, "flow %s: open, %s %s\n", stats.id.c_str(), declared.mediaType.c_str(),
                    stats.continuous ? "continuous" : "discrete");
                status = stats.continuous ? readContinuous(reader, info.config, declared, !wrong.empty(), stats)
                                          : readDiscrete(reader, info.config, declared, !wrong.empty(), stats);
            }
            mxlReleaseFlowReader(instance, reader);
            if (stopping)
            {
                break;
            }
            if (status == MXL_ERR_PERMISSION_DENIED)
            {
                ++stats.readPermission;
                stats.state = State::PermissionDenied;
                stats.error("permission denied reading the flow");
            }
            else if (status == MXL_ERR_FLOW_INVALID || status == MXL_ERR_INVALID_FLOW_READER || status == MXL_ERR_FLOW_NOT_FOUND)
            {
                // Its writer went away or replaced the flow: open it again.
                ++stats.lost;
                stats.state = State::FlowNotFound;
                stats.error("flow went away, reopening");
            }
            else
            {
                ++stats.readOther;
                stats.state = State::FlowError;
                stats.error(fmt::format("read failed: status {}, reopening", static_cast<int>(status)));
            }
            mxlSleepForNs(NsPerSecond);
        }
        if (instance != nullptr)
        {
            mxlDestroyInstance(instance);
        }
    }

    // Delivering means a read landed within the last second. A flow that is
    // open but silent is the failure this reader exists to show.
    bool delivering(FlowStats const& stats)
    {
        auto const last = stats.lastReadAt.load();
        return stats.state == State::Open && last != 0 && mxlGetTime() - last < NsPerSecond;
    }

    std::string jsonString(std::string const& value)
    {
        return picojson::value{value}.serialize();
    }

    struct Counter
    {
        char const* key;
        std::atomic<std::uint64_t> FlowStats::*field;
    };

    // Read outcomes, per grain or per sample. The key is what Grafana and the
    // page group by.
    constexpr Counter ReadResults[] = {
        {"ok",               &FlowStats::ok},
        {"too_early",        &FlowStats::tooEarly},
        {"too_late",         &FlowStats::tooLate},
        {"invalid",          &FlowStats::invalid},
        {"incomplete",       &FlowStats::incomplete},
        {"wrong_size",       &FlowStats::wrongSize},
        {"wrong_format",     &FlowStats::wrongFormat},
        {"illegal_values",   &FlowStats::illegal},
        {"permission_error", &FlowStats::readPermission},
        {"other_error",      &FlowStats::readOther},
    };

    constexpr Counter OpenFailures[] = {
        {"domain",     &FlowStats::domainErrors},
        {"not_found",  &FlowStats::flowNotFound},
        {"permission", &FlowStats::flowPermission},
        {"other",      &FlowStats::flowOther},
    };

    struct Single
    {
        char const* key;
        char const* name;
        char const* help;
        std::atomic<std::uint64_t> FlowStats::*field;
    };

    constexpr Single Singles[] = {
        {"skipped", "mxl_reader_skipped_total", "Grains or samples jumped over to reach the head again after the ring overtook the reader.", &FlowStats::skipped},
        {"stalls",  "mxl_reader_stalls_total",  "Read timeouts that passed with no read at all.",                                         &FlowStats::stalls},
        {"opens",   "mxl_reader_opens_total",   "Times the flow was opened.",                                                             &FlowStats::opens},
        {"lost",    "mxl_reader_lost_total",    "Times an open flow went away and was reopened.",                                         &FlowStats::lost},
    };

    std::string metrics(std::vector<std::unique_ptr<FlowStats>> const& flows)
    {
        std::string out;
        out += "# HELP mxl_reader_flow_info Media type and unit of a flow, once it has been opened.\n"
               "# TYPE mxl_reader_flow_info gauge\n";
        for (auto const& flow : flows)
        {
            if (flow->opens > 0)
            {
                auto const lock = std::lock_guard{flow->mutex};
                out += fmt::format("mxl_reader_flow_info{{flow=\"{}\",media_type=\"{}\",unit=\"{}\"}} 1\n", flow->id,
                    flow->mediaType, flow->continuous ? "samples" : "grains");
            }
        }
        out += "# HELP mxl_reader_flow_state 1 for the state the flow's reader is in.\n"
               "# TYPE mxl_reader_flow_state gauge\n";
        for (auto const& flow : flows)
        {
            out += fmt::format("mxl_reader_flow_state{{flow=\"{}\",state=\"{}\"}} 1\n", flow->id, stateName(flow->state));
        }
        out += "# HELP mxl_reader_flow_delivering 1 while reads land within a second.\n"
               "# TYPE mxl_reader_flow_delivering gauge\n";
        for (auto const& flow : flows)
        {
            out += fmt::format("mxl_reader_flow_delivering{{flow=\"{}\"}} {}\n", flow->id, delivering(*flow) ? 1 : 0);
        }
        out += "# HELP mxl_reader_reads_total Reads by outcome, in grains (discrete) or samples (continuous); "
               "too_early and too_late count events.\n"
               "# TYPE mxl_reader_reads_total counter\n";
        for (auto const& flow : flows)
        {
            for (auto const& c : ReadResults)
            {
                out += fmt::format("mxl_reader_reads_total{{flow=\"{}\",result=\"{}\"}} {}\n", flow->id, c.key, ((*flow).*c.field).load());
            }
        }
        out += "# HELP mxl_reader_open_failures_total Attempts to reach a flow that failed, by reason.\n"
               "# TYPE mxl_reader_open_failures_total counter\n";
        for (auto const& flow : flows)
        {
            for (auto const& c : OpenFailures)
            {
                out += fmt::format("mxl_reader_open_failures_total{{flow=\"{}\",reason=\"{}\"}} {}\n", flow->id, c.key, ((*flow).*c.field).load());
            }
        }
        for (auto const& s : Singles)
        {
            out += fmt::format("# HELP {} {}\n# TYPE {} counter\n", s.name, s.help, s.name);
            for (auto const& flow : flows)
            {
                out += fmt::format("{}{{flow=\"{}\"}} {}\n", s.name, flow->id, ((*flow).*s.field).load());
            }
        }
        out += "# HELP mxl_reader_age_seconds Wall clock minus the media timestamp of the last read.\n"
               "# TYPE mxl_reader_age_seconds gauge\n";
        for (auto const& flow : flows)
        {
            if (flow->lastReadAt != 0)
            {
                out += fmt::format("mxl_reader_age_seconds{{flow=\"{}\"}} {:.6f}\n", flow->id, flow->ageNs / 1e9);
            }
        }
        return out;
    }

    // The same counts as /metrics, shaped for a page: one object per flow with
    // its state, what it declares, and its last error.
    std::string status(std::vector<std::unique_ptr<FlowStats>> const& flows)
    {
        auto const now = mxlGetTime();
        std::string out = "{\"flows\":[";
        for (std::size_t i = 0; i < flows.size(); ++i)
        {
            auto const& flow = *flows[i];
            auto const lock = std::lock_guard{flow.mutex};
            out += i ? "," : "";
            out += fmt::format("{{\"id\":{},\"state\":\"{}\",\"delivering\":{},\"unit\":\"{}\",\"mediaType\":{},"
                               "\"format\":{},\"formatMismatch\":{},\"ageSeconds\":{},",
                jsonString(flow.id), stateName(flow.state), delivering(flow) ? "true" : "false",
                flow.continuous ? "samples" : "grains", jsonString(flow.mediaType), jsonString(flow.format),
                jsonString(flow.formatMismatch),
                flow.lastReadAt ? fmt::format("{:.6f}", flow.ageNs / 1e9) : std::string{"null"});
            out += "\"reads\":{";
            for (std::size_t c = 0; c < std::size(ReadResults); ++c)
            {
                out += fmt::format("{}\"{}\":{}", c ? "," : "", ReadResults[c].key, (flow.*ReadResults[c].field).load());
            }
            out += "},\"openFailures\":{";
            for (std::size_t c = 0; c < std::size(OpenFailures); ++c)
            {
                out += fmt::format("{}\"{}\":{}", c ? "," : "", OpenFailures[c].key, (flow.*OpenFailures[c].field).load());
            }
            out += "}";
            for (auto const& s : Singles)
            {
                out += fmt::format(",\"{}\":{}", s.key, (flow.*s.field).load());
            }
            out += flow.lastErrorAt ? fmt::format(",\"lastError\":{{\"message\":{},\"secondsAgo\":{:.1f}}}}}", jsonString(flow.lastError),
                                          (now - flow.lastErrorAt) / 1e9)
                                    : std::string{",\"lastError\":null}"};
        }
        return out + "]}";
    }

    void serve(int port, std::vector<std::unique_ptr<FlowStats>> const& flows)
    {
        auto const fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0)
        {
            std::fprintf(stderr, "cannot open a socket\n");
            failed = true;
            stopping = true;
            return;
        }
        auto const one = 1;
        ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        auto addr = sockaddr_in{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        addr.sin_port = htons(static_cast<std::uint16_t>(port));
        if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 || ::listen(fd, 16) != 0)
        {
            std::fprintf(stderr, "cannot listen on :%d\n", port);
            failed = true;
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
            // Short, so one client that sends nothing cannot hold a probe past
            // its own timeout: requests are answered one at a time.
            auto const clientTv = timeval{.tv_sec = 0, .tv_usec = 200'000};
            ::setsockopt(client, SOL_SOCKET, SO_RCVTIMEO, &clientTv, sizeof(clientTv));
            ::setsockopt(client, SOL_SOCKET, SO_SNDTIMEO, &clientTv, sizeof(clientTv));
            char request[1024] = {};
            auto const n = ::recv(client, request, sizeof(request) - 1, 0);
            // The request target alone, without a query: "GET <path> HTTP/1.1".
            auto line = std::string{n > 0 ? request : ""};
            line = line.substr(0, line.find_first_of("\r\n"));
            auto const target = line.starts_with("GET ") ? line.substr(4, line.find(' ', 4) - 4) : std::string{};
            auto const path = target.substr(0, target.find('?'));

            auto code = 200;
            auto type = std::string{"text/plain; version=0.0.4"};
            std::string body;
            if (path == "/metrics")
            {
                body = metrics(flows);
            }
            else if (path == "/status")
            {
                body = status(flows);
                type = "application/json";
            }
            else if (path == "/delivering")
            {
                auto all = true;
                for (auto const& flow : flows)
                {
                    all = all && delivering(*flow);
                }
                code = all ? 200 : 503;
                body = all ? "delivering\n" : "not every flow is delivering\n";
            }
            else if (path == "/healthz")
            {
                body = "ok\n";
            }
            else
            {
                code = 404;
                body = "not found\n";
            }
            auto const response = fmt::format("HTTP/1.1 {} {}\r\nContent-Type: {}\r\n"
                                              "Content-Length: {}\r\nConnection: close\r\n\r\n{}",
                code, code == 200 ? "OK" : code == 503 ? "Service Unavailable" : "Not Found", type, body.size(), body);
            ::send(client, response.data(), response.size(), MSG_NOSIGNAL);
            ::close(client);
        }
        ::close(fd);
    }

    // --expect '{"<flow id>": {"mediaType": "video/v210", "width": 1920,
    // "height": 1080, "rate": "50/1", "checkLegal": true}, ...}'; audio takes
    // sampleRate and channels. Returns an error, or "" when the document is
    // usable.
    std::string parseExpectations(std::string const& text, std::map<std::string, Expectation>& out)
    {
        if (text.empty())
        {
            return {};
        }
        auto json = picojson::value{};
        if (auto const err = picojson::parse(json, text); !err.empty())
        {
            return "--expect is not JSON: " + err;
        }
        if (!json.is<picojson::object>())
        {
            return "--expect must be an object keyed by flow id";
        }
        for (auto const& [id, value] : json.get<picojson::object>())
        {
            if (!value.is<picojson::object>())
            {
                return "--expect: the expectation for " + id + " must be an object";
            }
            auto const& o = value.get<picojson::object>();
            auto e = Expectation{};
            if (auto const it = o.find("mediaType"); it != o.end() && it->second.is<std::string>())
            {
                e.mediaType = it->second.get<std::string>();
            }
            if (auto const it = o.find("rate"); it != o.end() && it->second.is<std::string>())
            {
                e.rate = it->second.get<std::string>();
            }
            e.width = jsonInt(o, "width");
            e.height = jsonInt(o, "height");
            e.sampleRate = jsonInt(o, "sampleRate");
            e.channels = jsonInt(o, "channels");
            if (auto const it = o.find("checkLegal"); it != o.end() && it->second.is<bool>())
            {
                e.checkLegal = it->second.get<bool>();
            }
            out[id] = e;
        }
        return {};
    }
}

int main(int argc, char** argv)
{
    auto app = CLI::App{"mxl-reader"};
    auto domain = std::string{};
    app.add_option("-d,--domain", domain, "The MXL domain directory.")->required();
    auto flowIds = std::vector<std::string>{};
    app.add_option("-f,--flow", flowIds, "A flow id to read. Repeat for several.")->required();
    auto expect = std::string{};
    app.add_option("--expect", expect, "JSON object of what each flow should carry, keyed by flow id.");
    auto port = 9102;
    app.add_option("-p,--port", port, "Port of the metrics and status endpoints.");
    auto reportSeconds = 10;
    app.add_option("--report-interval", reportSeconds, "Seconds between summary lines on stderr. 0 disables them.");
    CLI11_PARSE(app, argc, argv);

    // libmxl answers a malformed id as an invalid argument, which the read loop
    // would take for a flow not there yet and wait on for ever.
    auto const uuid = std::regex{"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"};
    for (auto const& id : flowIds)
    {
        if (!std::regex_match(id, uuid))
        {
            std::fprintf(stderr, "not a lowercase flow UUID: %s\n", id.c_str());
            return 2;
        }
    }
    auto expectations = std::map<std::string, Expectation>{};
    if (auto const err = parseExpectations(expect, expectations); !err.empty())
    {
        std::fprintf(stderr, "%s\n", err.c_str());
        return 2;
    }

    std::signal(SIGTERM, onSignal);
    std::signal(SIGINT, onSignal);

    auto flows = std::vector<std::unique_ptr<FlowStats>>{};
    for (auto const& id : flowIds)
    {
        flows.push_back(std::make_unique<FlowStats>());
        flows.back()->id = id;
        if (auto const it = expectations.find(id); it != expectations.end())
        {
            flows.back()->expected = it->second;
        }
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
            auto const ok = flow.ok.load();
            std::fprintf(stderr,
                "flow %s: %s, %.1f %s/s ok, early %llu, late %llu, invalid %llu, wrong %llu, illegal %llu, stalls %llu, age %.3f s\n",
                flow.id.c_str(), delivering(flow) ? "delivering" : stateName(flow.state),
                static_cast<double>(ok - previous[i]) / reportSeconds, flow.continuous ? "samples" : "grains",
                static_cast<unsigned long long>(flow.tooEarly.load()), static_cast<unsigned long long>(flow.tooLate.load()),
                static_cast<unsigned long long>(flow.invalid.load()),
                static_cast<unsigned long long>(flow.wrongSize.load() + flow.wrongFormat.load()),
                static_cast<unsigned long long>(flow.illegal.load()), static_cast<unsigned long long>(flow.stalls.load()),
                flow.ageNs / 1e9);
            previous[i] = ok;
        }
    }

    for (auto& thread : threads)
    {
        thread.join();
    }
    return failed ? 1 : 0;
}
