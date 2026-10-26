// mxl-reader: keeps MXL flows open and reads every grain or sample batch as it
// lands, so the node it runs on holds a live consumer of each flow.
//
// A consumer is what makes mxl-k8s replicate a flow to a node, and reading at
// the head is what shows whether that replica keeps up. Each flow is read on a
// thread of its own. Every read is classified - correct, not there when due,
// overtaken, unreadable, or there but wrong - and counted, so a load or failure
// test sees exactly which way a flow went bad. The classification, the checks
// and the counts are libmxl-monitoring's; they are served as Prometheus text
// on /metrics and as JSON on /status; /delivering answers 200 only while every
// connected flow delivers.
//
// With --nmos-seed the reader is also an NMOS Node (AMWA IS-04, IS-05 and
// BCP-007-03): each named reader slot is an MXL Receiver a controller connects
// to a flow, and a connection moves that slot's reader to the flow named.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <deque>
#include <limits>
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
#include <mxl-monitoring/content.h>
#include <mxl-monitoring/monitoring.h>
#include <mxl-monitoring/prometheus.h>
#include <mxl-monitoring/reader.h>
#include <mxl/flow.h>
#include <mxl/flowinfo.h>
#include <mxl/mxl.h>
#include <mxl/time.h>
#include <picojson/wrapper.h>

#include "checks.hpp"
#include "http.hpp"
#include "nmos.hpp"
#include "page.hpp"

namespace
{
    using reader::Expectation;

    std::atomic<bool> stopping{false};
    // Set by a thread that cannot do its job at all, so the process exits
    // non-zero instead of looking like a clean shutdown.
    std::atomic<bool> failed{false};

    void onSignal(int)
    {
        stopping = true;
    }

    constexpr std::uint64_t NsPerSecond = 1'000'000'000ULL;

#ifndef MXL_READER_VERSION
#define MXL_READER_VERSION "dev"
#endif

    // This build and the libmxl it runs against: the second is what decides
    // how a flow is read, and the image's own tag does not name it.
    std::string versionText()
    {
        auto lib = mxlVersionType{};
        mxlGetVersion(&lib);
        return fmt::format("mxl-reader {} (libmxl {})", MXL_READER_VERSION, lib.full != nullptr ? lib.full : "unknown");
    }

    // One thing that happened to a flow, for the ring view: a grain or a batch
    // of samples read, or a timing event. Times are TAI nanoseconds, latencies
    // relative to the media timestamp of the index.
    struct GrainEvent
    {
        std::uint64_t seq = 0;
        std::uint64_t index = 0;
        std::uint64_t seenNs = 0;
        std::int64_t arrivalNs = 0;
        // Committed on this node, by the writer or the mirror. Known only for a
        // grain read while it is the head: libmxl stamps the flow, not the
        // grain, with its last write.
        std::int64_t commitNs = Unknown;
        std::int64_t behind = 0;
        // k ok, f wrong format, m missing, v invalid, p incomplete, s wrong
        // size, x illegal values, l too late, e too early, t stall.
        char kind = 'k';
        std::uint64_t count = 1;

        static constexpr std::int64_t Unknown = std::numeric_limits<std::int64_t>::min();
    };

    // The ring view's window: enough for several seconds of a 240/1 flow.
    constexpr std::size_t EventsKept = 4096;

    // A reader slot. What is counted of it - reads, open failures, state, age
    // - is libmxl-monitoring's, in `monitor`; what is kept here is what the
    // ring view and /status show beyond that.
    struct FlowStats
    {
        // What the slot is keyed by and reported as: its flow id, or its name
        // for a named slot, which keeps it whichever flow it reads.
        std::string id;
        std::optional<Expectation> expected;
        mxlMonitoringFlow monitor = nullptr;

        // The flow the slot reads; empty reads nothing. An NMOS controller
        // can change it while the slot runs: written under mutex, and every
        // change counts up generation, which ends the read loop of the flow
        // before.
        std::string target;
        std::atomic<std::uint64_t> generation{0};

        std::atomic<bool> continuous{false};

        // What the ring view draws against: the ring's length in grains or
        // samples, the payload bytes of one slot (a grain, or one sample of
        // every channel), one grain or sample period, the too-early grace, and
        // where the reader and the head stand. The slot size is known once a
        // read has returned data.
        std::atomic<std::uint64_t> ringLength{0};
        std::atomic<std::uint64_t> slotBytes{0};
        std::atomic<std::uint64_t> periodNs{0};
        std::atomic<std::uint64_t> graceNs{0};
        std::atomic<std::uint64_t> readerIndex{0};
        std::atomic<std::uint64_t> headSeen{0};
        // Whether the ring view has been shown the current stall.
        std::atomic<bool> stallShown{false};

        // Read by the HTTP thread, written by the flow's own.
        mutable std::mutex mutex;
        std::string format;
        std::string lastError;
        std::uint64_t lastErrorAt = 0;
        std::deque<GrainEvent> events;
        std::uint64_t eventSeq = 0;

        void record(GrainEvent event)
        {
            auto const lock = std::lock_guard{mutex};
            event.seq = ++eventSeq;
            events.push_back(event);
            if (events.size() > EventsKept)
            {
                events.pop_front();
            }
        }

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

    std::pair<std::string, std::uint64_t> targetOf(FlowStats& stats)
    {
        auto const lock = std::lock_guard{stats.mutex};
        return {stats.target, stats.generation.load()};
    }

    // Forgets what the ring view and /status showed of the flow before; the
    // library forgets its part when the reader thread retargets the slot.
    // Called with mutex held.
    void forgetFlow(FlowStats& stats)
    {
        stats.format.clear();
        stats.lastError.clear();
        stats.lastErrorAt = 0;
        stats.events.clear();
        stats.ringLength = 0;
        stats.slotBytes = 0;
        stats.readerIndex = 0;
        stats.headSeen = 0;
    }

    // Moves a slot to another flow, or off its flow for an empty id.
    void retarget(FlowStats& stats, std::string const& flow)
    {
        auto const lock = std::lock_guard{stats.mutex};
        if (stats.target != flow)
        {
            stats.target = flow;
            ++stats.generation;
            forgetFlow(stats);
        }
    }

    // The reader thread forgets the flow it leaves as well, once it has
    // stopped reading it, and points the slot's monitoring at the new one.
    void leaveFlow(FlowStats& stats, std::string const& flow)
    {
        mxlMonitoringSetFlowTarget(stats.monitor, flow.c_str());
        auto const lock = std::lock_guard{stats.mutex};
        forgetFlow(stats);
    }

    // Sleeps up to ns, but wakes as soon as the slot is moved to another flow,
    // so a retry interval never delays a connection.
    void pause(FlowStats const& stats, std::uint64_t generation, std::uint64_t ns)
    {
        while (ns > 0 && !stopping && stats.generation == generation)
        {
            auto const step = std::min<std::uint64_t>(ns, NsPerSecond / 20);
            mxlSleepForNs(step);
            ns -= step;
        }
    }

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

    // Nothing to read yet. That is the normal state of a reader at the head
    // until the next grain is due; the library counts it as too early only
    // once the index is a grace period past due, and once per index, and a
    // stall once per read timeout with no read at all. The ring view shows
    // both by the same rules. The sleep lasts until the index is due, at most
    // 5 ms, so a non-blocking read cannot spin. Returns whether a whole
    // timeout has passed without a read.
    bool awaitIndex(FlowStats& stats, mxlStatus status, mxlRational const& rate, std::uint64_t index, std::uint64_t graceNs,
        std::uint64_t timeoutNs, std::uint64_t& earlyShown)
    {
        mxlMonitoringRecordRead(stats.monitor, status, index, 1);
        auto const now = mxlGetTime();
        auto const due = mxlIndexToTimestamp(&rate, index);
        if (earlyShown != index && now > due + graceNs)
        {
            earlyShown = index;
            stats.record({.index = index, .seenNs = now, .arrivalNs = static_cast<std::int64_t>(now - due), .kind = 'e'});
        }
        auto const progress = mxlMonitoringFlowLastProgress(stats.monitor);
        auto const timedOut = now >= progress && now - progress >= timeoutNs;
        if (timedOut && !stats.stallShown.exchange(true))
        {
            stats.record({.index = index, .seenNs = now, .arrivalNs = static_cast<std::int64_t>(now - due), .kind = 't'});
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

    char kindOf(mxlMonitoringContent issue)
    {
        switch (issue)
        {
            case MXL_MONITORING_CONTENT_WRONG_FORMAT:
            case MXL_MONITORING_CONTENT_WRONG_RESOLUTION:
            case MXL_MONITORING_CONTENT_WRONG_RATE:       return 'f';
            case MXL_MONITORING_CONTENT_WRONG_SIZE:       return 's';
            case MXL_MONITORING_CONTENT_MISSING:          return 'm';
            case MXL_MONITORING_CONTENT_INVALID:          return 'v';
            case MXL_MONITORING_CONTENT_INCOMPLETE:       return 'p';
            case MXL_MONITORING_CONTENT_ILLEGAL_VALUES:   return 'x';
            case MXL_MONITORING_CONTENT_COUNT:            break;
        }
        return 'k';
    }

    // What the checks found in data read at index: counted with the library
    // as good or as the one issue it has, and returned as its ring view kind.
    char count(FlowStats& stats, bool found, mxlMonitoringContent issue, char const* where, std::uint64_t index, std::uint64_t units)
    {
        if (!found)
        {
            mxlMonitoringRecordRead(stats.monitor, MXL_STATUS_OK, index, units);
            stats.stallShown = false;
            return 'k';
        }
        if (issue == MXL_MONITORING_CONTENT_ILLEGAL_VALUES)
        {
            stats.error(fmt::format("illegal sample value: {}", where));
        }
        mxlMonitoringRecordReadContent(stats.monitor, issue, index, units);
        stats.stallShown = false;
        return kindOf(issue);
    }

    // The event for an index just read: when it was seen, and, when it is the
    // head, when it was committed on this node, which the library takes as
    // the commit latency.
    GrainEvent readEvent(mxlFlowReader reader, mxlRational const& rate, std::uint64_t index, char kind, std::uint64_t count,
        FlowStats& stats)
    {
        auto const now = mxlGetTime();
        auto const due = static_cast<std::int64_t>(mxlIndexToTimestamp(&rate, index));
        auto event = GrainEvent{.index = index, .seenNs = now, .arrivalNs = static_cast<std::int64_t>(now) - due, .kind = kind, .count = count};
        auto runtime = mxlFlowRuntimeInfo{};
        if (mxlFlowReaderGetRuntimeInfo(reader, &runtime) == MXL_STATUS_OK)
        {
            stats.headSeen = runtime.headIndex;
            event.behind = static_cast<std::int64_t>(runtime.headIndex) - static_cast<std::int64_t>(index);
            if (runtime.headIndex == index && runtime.lastWriteTime != 0)
            {
                event.commitNs = static_cast<std::int64_t>(runtime.lastWriteTime) - due;
                mxlMonitoringRecordCommit(stats.monitor, index, runtime.lastWriteTime);
            }
        }
        stats.readerIndex = index;
        return event;
    }

    // What one open of a flow reads against: its definition, how it differs
    // from what was expected, and whether sample values are checked.
    struct Checks
    {
        mxlMonitoringDeclared declared{};
        std::uint32_t mismatch = 0;
        bool checkLegal = false;
    };

    // Returns the status that ended the loop: anything other than a read that
    // is merely early or late means the reader no longer holds a usable flow.
    // The library has counted it by then.
    mxlStatus readDiscrete(mxlFlowReader reader, mxlFlowConfigInfo const& config, Checks const& checks, FlowStats& stats,
        std::uint64_t generation)
    {
        auto const& rate = config.common.grainRate;
        // Several grain periods, so a writer that commits a grain late is a
        // slow grain rather than a stall, while a stopped writer still shows.
        auto const period = periodNs(rate);
        auto const timeoutNs = std::max<std::uint64_t>(4 * period, NsPerSecond / 10);
        auto const grace = graceNs(config, period, false);
        mxlMonitoringSetFlowTiming(stats.monitor, grace, timeoutNs);
        stats.ringLength = config.discrete.grainCount;
        stats.periodNs = period;
        stats.graceNs = grace;
        auto index = headIndex(reader, mxlGetCurrentIndex(&rate));
        auto earlyShown = std::uint64_t{0};
        auto const take = [&](mxlGrainInfo const& grain, std::uint8_t const* payload) {
            auto issue = mxlMonitoringContent{};
            auto where = std::array<char, MXL_MONITORING_TEXT>{};
            auto const found = mxlMonitoringCheckGrain(&checks.declared, checks.mismatch, checks.checkLegal, &grain, index, payload, &issue,
                where.data(), where.size());
            auto const kind = count(stats, found, issue, where.data(), index, 1);
            if (grain.grainSize > 0)
            {
                stats.slotBytes = grain.grainSize;
            }
            stats.record(readEvent(reader, rate, index, kind, 1, stats));
            ++index;
        };
        while (!stopping && stats.generation == generation)
        {
            auto grain = mxlGrainInfo{};
            std::uint8_t* payload = nullptr;
            // Non-blocking, as for samples, and whole grains only: a blocking
            // read sleeps to its deadline whenever a writer signals less often
            // than it commits.
            auto const status = mxlFlowReaderGetGrainSliceNonBlocking(reader, index, MXL_GRAIN_VALID_SLICES_ALL, &grain, &payload);
            switch (status)
            {
                case MXL_STATUS_OK: take(grain, payload); break;
                case MXL_ERR_OUT_OF_RANGE_TOO_LATE:
                {
                    // The ring overwrote the grain before it was read: jump to
                    // the head and count what was lost on the way.
                    mxlMonitoringRecordRead(stats.monitor, status, index, 1);
                    auto const head = headIndex(reader, mxlGetCurrentIndex(&rate));
                    auto const lost = head > index ? head - index : 1;
                    mxlMonitoringRecordSkipped(stats.monitor, lost);
                    stats.record({.index = index, .seenNs = mxlGetTime(), .behind = static_cast<std::int64_t>(head - index), .kind = 'l',
                        .count = lost});
                    index = head > index ? head : index + 1;
                    break;
                }
                case MXL_ERR_OUT_OF_RANGE_TOO_EARLY:
                case MXL_ERR_TIMEOUT:
                {
                    // A grain the writer moved on from without finishing will
                    // never be whole: once a later grain is written, take it as
                    // it stands so it counts as incomplete instead of holding the
                    // reader until the ring overwrites it.
                    if (auto const head = headIndex(reader, 0); head > index)
                    {
                        auto partial = mxlGrainInfo{};
                        std::uint8_t* partialPayload = nullptr;
                        if (mxlFlowReaderGetGrainSliceNonBlocking(reader, index, MXL_GRAIN_VALID_SLICES_ANY, &partial, &partialPayload) ==
                            MXL_STATUS_OK)
                        {
                            take(partial, partialPayload);
                            break;
                        }
                    }
                    // A writer whose clock stepped back leaves its head behind
                    // the index waited on; follow the head once a whole timeout
                    // has passed instead of waiting for the clock to catch up.
                    if (awaitIndex(stats, status, rate, index, grace, timeoutNs, earlyShown))
                    {
                        if (auto const head = headIndex(reader, index); head + 1 < index)
                        {
                            index = head + 1;
                        }
                    }
                    break;
                }
                default: mxlMonitoringRecordRead(stats.monitor, status, index, 1); return status;
            }
        }
        return MXL_STATUS_OK;
    }

    mxlStatus readContinuous(mxlFlowReader reader, mxlFlowConfigInfo const& config, Checks const& checks, FlowStats& stats,
        std::uint64_t generation)
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
        stats.ringLength = config.continuous.bufferLength;
        stats.periodNs = periodNs(rate);
        auto const timeoutNs = std::max<std::uint64_t>(4 * batchNs, NsPerSecond / 10);
        auto const grace = graceNs(config, batchNs, true);
        mxlMonitoringSetFlowTiming(stats.monitor, grace, timeoutNs);
        stats.graceNs = grace;
        auto index = headIndex(reader, mxlGetCurrentIndex(&rate));
        auto earlyShown = std::uint64_t{0};
        while (!stopping && stats.generation == generation)
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
                {
                    auto channelBytes = std::uint64_t{0};
                    for (auto const& fragment : slices.base.fragments)
                    {
                        channelBytes += fragment.size;
                    }
                    if (channelBytes > 0)
                    {
                        stats.slotBytes = channelBytes / batch * slices.count;
                    }
                    auto issue = mxlMonitoringContent{};
                    auto where = std::array<char, MXL_MONITORING_TEXT>{};
                    auto const found = mxlMonitoringCheckSamples(&checks.declared, checks.mismatch, checks.checkLegal, &slices, &issue,
                        where.data(), where.size());
                    auto const kind = count(stats, found, issue, where.data(), next, batch);
                    stats.record(readEvent(reader, rate, next, kind, batch, stats));
                    index = next;
                    break;
                }
                case MXL_ERR_OUT_OF_RANGE_TOO_LATE:
                {
                    mxlMonitoringRecordRead(stats.monitor, status, index, 1);
                    auto const head = headIndex(reader, mxlGetCurrentIndex(&rate));
                    auto const lost = head > index ? head - index : batch;
                    mxlMonitoringRecordSkipped(stats.monitor, lost);
                    stats.record({.index = index, .seenNs = mxlGetTime(), .behind = static_cast<std::int64_t>(head - index), .kind = 'l',
                        .count = lost});
                    index = head > index ? head : next;
                    break;
                }
                case MXL_ERR_OUT_OF_RANGE_TOO_EARLY:
                case MXL_ERR_TIMEOUT:
                    if (awaitIndex(stats, status, rate, next, grace, timeoutNs, earlyShown))
                    {
                        if (auto const head = headIndex(reader, index); head < index)
                        {
                            index = head;
                        }
                    }
                    break;
                default: mxlMonitoringRecordRead(stats.monitor, status, next, batch); return status;
            }
        }
        return MXL_STATUS_OK;
    }

    void readFlow(std::string const& domain, FlowStats& stats)
    {
        mxlInstance instance = nullptr;
        auto seen = stats.generation.load();
        while (!stopping)
        {
            auto const [flow, generation] = targetOf(stats);
            if (generation != seen)
            {
                // Whatever this thread recorded of the flow it left, after
                // retarget() cleared it, goes too: a read or an error that was
                // under way when the slot moved.
                leaveFlow(stats, flow);
                seen = generation;
            }
            if (flow.empty())
            {
                pause(stats, generation, NsPerSecond);
                continue;
            }
            if (instance == nullptr)
            {
                // The volume may not be mounted yet, or the agent has not
                // created the domain: wait for it rather than exit.
                instance = mxlMonitoringCreateInstance(stats.monitor, domain.c_str(), "");
                if (instance == nullptr)
                {
                    struct stat st{};
                    stats.error(::stat(domain.c_str(), &st) == 0 ? fmt::format("cannot open MXL domain {}", domain)
                                                                  : fmt::format("MXL domain {} does not exist", domain));
                    pause(stats, generation, NsPerSecond);
                    continue;
                }
            }

            // Not there yet: the flow is still being mirrored here, or its
            // writer has not started. Keep asking rather than exiting, so the
            // pod waits for the flow instead of crash-looping until it lands.
            // A reader that opens but cannot read the flow's info or
            // definition is released again: nothing can be checked against a
            // flow that is not described.
            mxlFlowReader reader = nullptr;
            auto status = mxlMonitoringCreateFlowReader(stats.monitor, instance, "", &reader);
            auto info = mxlFlowInfo{};
            auto checks = Checks{};
            if (status == MXL_STATUS_OK)
            {
                status = mxlFlowReaderGetInfo(reader, &info);
                if (status != MXL_STATUS_OK)
                {
                    mxlReleaseFlowReader(instance, reader);
                    auto const message = fmt::format("cannot read the info of flow {}: status {}", flow, static_cast<int>(status));
                    mxlMonitoringDescribe(stats.monitor, MXL_MONITORING_CONDITION_ACCESS, MXL_MONITORING_ACCESS_OTHER, MXL_MONITORING_CONTENT_COUNT,
                        message.c_str());
                    mxlMonitoringRecordAccess(stats.monitor, MXL_MONITORING_ACCESS_OTHER, status);
                }
                mxlMonitoringGetDeclared(stats.monitor, &checks.declared);
            }
            if (status != MXL_STATUS_OK)
            {
                // The library said what failed, naming the flow.
                auto detail = std::array<char, MXL_MONITORING_TEXT>{};
                mxlMonitoringGetLastErrorDetail(stats.monitor, detail.data(), detail.size());
                stats.error(detail.data());
                pause(stats, generation, NsPerSecond);
                continue;
            }
            if (stats.generation != generation)
            {
                // Moved while opening. The library has counted the open; the
                // reader thread forgets the flow when it takes up the next.
                mxlReleaseFlowReader(instance, reader);
                continue;
            }
            {
                auto const& declared = checks.declared;
                auto wrong = std::array<char, MXL_MONITORING_TEXT>{};
                if (stats.expected)
                {
                    auto const expected = reader::toMonitoring(*stats.expected);
                    checks.mismatch = mxlMonitoringMismatch(&declared, &expected, wrong.data(), wrong.size());
                    checks.checkLegal = stats.expected->checkLegal;
                }
                {
                    auto const lock = std::lock_guard{stats.mutex};
                    auto const rate = declared.rate.denominator > 0 ? fmt::format("{}/{}", declared.rate.numerator, declared.rate.denominator)
                                                                    : std::string{};
                    stats.format = declared.width ? fmt::format("{}x{} @ {}", declared.width, declared.height, rate)
                                 : declared.sampleRate ? fmt::format("{} ch @ {}", declared.channels, declared.sampleRate)
                                                       : std::string{};
                }
                if (checks.mismatch != 0)
                {
                    stats.error(fmt::format("format differs from what was expected: {}", wrong.data()));
                }
                stats.continuous = info.config.common.format == MXL_DATA_FORMAT_AUDIO;
                std::fprintf(stderr, "flow %s: open %s, %s %s\n", stats.id.c_str(), flow.c_str(), declared.mediaType,
                    stats.continuous ? "continuous" : "discrete");
                status = stats.continuous ? readContinuous(reader, info.config, checks, stats, generation)
                                          : readDiscrete(reader, info.config, checks, stats, generation);
            }
            mxlReleaseFlowReader(instance, reader);
            if (stopping)
            {
                break;
            }
            if (stats.generation != generation)
            {
                // Moved to another flow: not a failure of this one.
                std::fprintf(stderr, "flow %s: leaving %s for another flow\n", stats.id.c_str(), flow.c_str());
                continue;
            }
            // The library counted it when it was read.
            if (status == MXL_ERR_PERMISSION_DENIED)
            {
                stats.error("permission denied reading flow " + flow);
            }
            else if (status == MXL_ERR_FLOW_INVALID || status == MXL_ERR_INVALID_FLOW_READER || status == MXL_ERR_FLOW_NOT_FOUND)
            {
                // Its writer went away or replaced the flow: open it again.
                stats.error("flow " + flow + " went away, reopening");
            }
            else
            {
                stats.error(fmt::format("reading flow {} failed: status {}, reopening", flow, static_cast<int>(status)));
            }
            pause(stats, generation, NsPerSecond);
        }
        if (instance != nullptr)
        {
            mxlDestroyInstance(instance);
        }
    }

    std::string jsonString(std::string const& value)
    {
        return picojson::value{value}.serialize();
    }

    // Every slot's counts from the library, in the order the slots were added.
    std::vector<mxlMonitoringFlowSnapshot> snapshots(mxlMonitoring monitoring)
    {
        auto out = std::vector<mxlMonitoringFlowSnapshot>{};
        auto count = std::size_t{0};
        do
        {
            out.resize(count);
            count = out.size();
            mxlMonitoringSnapshot(monitoring, out.data(), &count);
        }
        while (count > out.size());
        out.resize(count);
        return out;
    }

    struct Counter
    {
        char const* key;
        std::uint64_t (*value)(mxlMonitoringFlowSnapshot const&);
    };

    // Read outcomes, per grain or per sample, under the keys /metrics uses.
    constexpr Counter ReadResults[] = {
        {"ok",               [](auto const& s) { return s.reads[MXL_MONITORING_READ_OK]; }                },
        {"too_early",        [](auto const& s) { return s.reads[MXL_MONITORING_READ_TOO_EARLY]; }         },
        {"too_late",         [](auto const& s) { return s.reads[MXL_MONITORING_READ_TOO_LATE]; }          },
        {"missing",          [](auto const& s) { return s.content[MXL_MONITORING_CONTENT_MISSING]; }      },
        {"invalid",          [](auto const& s) { return s.content[MXL_MONITORING_CONTENT_INVALID]; }      },
        {"incomplete",       [](auto const& s) { return s.content[MXL_MONITORING_CONTENT_INCOMPLETE]; }   },
        {"wrong_size",       [](auto const& s) { return s.content[MXL_MONITORING_CONTENT_WRONG_SIZE]; }   },
        {"wrong_format",
         [](auto const& s) {
             return s.content[MXL_MONITORING_CONTENT_WRONG_FORMAT] + s.content[MXL_MONITORING_CONTENT_WRONG_RESOLUTION] +
                    s.content[MXL_MONITORING_CONTENT_WRONG_RATE];
         }                                                                                                },
        {"illegal_values",   [](auto const& s) { return s.content[MXL_MONITORING_CONTENT_ILLEGAL_VALUES]; }},
        {"permission_error", [](auto const& s) { return s.reads[MXL_MONITORING_READ_PERMISSION_DENIED]; } },
        {"other_error",      [](auto const& s) { return s.reads[MXL_MONITORING_READ_ERROR]; }             },
    };

    constexpr Counter OpenFailures[] = {
        {"domain",     [](auto const& s) { return s.accessFailures[MXL_MONITORING_ACCESS_DOMAIN_UNAVAILABLE]; }  },
        {"not_found",  [](auto const& s) { return s.accessFailures[MXL_MONITORING_ACCESS_FLOW_NOT_FOUND]; }      },
        {"permission", [](auto const& s) { return s.accessFailures[MXL_MONITORING_ACCESS_PERMISSION_DENIED]; }   },
        {"other",
         [](auto const& s) { return s.accessFailures[MXL_MONITORING_ACCESS_FLOW_INVALID] + s.accessFailures[MXL_MONITORING_ACCESS_OTHER]; }},
    };

    constexpr Counter Singles[] = {
        {"skipped", [](auto const& s) { return s.skipped; }},
        {"stalls",  [](auto const& s) { return s.stalls; } },
        {"opens",   [](auto const& s) { return s.opens; }  },
        {"lost",    [](auto const& s) { return s.losses; } },
    };

    std::string metrics(mxlMonitoring monitoring)
    {
        auto size = std::size_t{0};
        mxlMonitoringPrometheusRender(monitoring, nullptr, &size);
        auto buffer = std::vector<char>(size);
        // Grown for flows added in between: the second call fits or says how much.
        while (mxlMonitoringPrometheusRender(monitoring, buffer.data(), &size) == MXL_ERR_INVALID_ARG && size > buffer.size())
        {
            buffer.resize(size);
        }
        return buffer.empty() ? std::string{} : std::string{buffer.data()};
    }

    // The same counts as /metrics, shaped for a page: one object per flow with
    // its state, what it declares, and its last error.
    std::string status(std::vector<std::unique_ptr<FlowStats>> const& flows, mxlMonitoring monitoring)
    {
        auto const now = mxlGetTime();
        auto const counts = snapshots(monitoring);
        std::string out = fmt::format("{{\"version\":{},\"flows\":[", jsonString(versionText()));
        for (std::size_t i = 0; i < flows.size() && i < counts.size(); ++i)
        {
            auto const& flow = *flows[i];
            auto const& s = counts[i];
            auto const lock = std::lock_guard{flow.mutex};
            out += i ? "," : "";
            out += fmt::format("{{\"id\":{},\"flowId\":{},\"state\":\"{}\",\"delivering\":{},\"unit\":{},\"mediaType\":{},"
                               "\"format\":{},\"formatMismatch\":{},\"ageSeconds\":{},",
                jsonString(flow.id), jsonString(flow.target), mxlMonitoringAccessName(s.access), s.delivering ? "true" : "false",
                s.opens == 0 ? "null" : flow.continuous ? "\"samples\"" : "\"grains\"",
                jsonString(s.mediaType), jsonString(flow.format), jsonString(s.formatMismatch),
                s.lastDataTimeNs ? fmt::format("{:.6f}", static_cast<double>(s.ageNs) / 1e9) : std::string{"null"});
            out += "\"reads\":{";
            for (std::size_t c = 0; c < std::size(ReadResults); ++c)
            {
                out += fmt::format("{}\"{}\":{}", c ? "," : "", ReadResults[c].key, ReadResults[c].value(s));
            }
            out += "},\"openFailures\":{";
            for (std::size_t c = 0; c < std::size(OpenFailures); ++c)
            {
                out += fmt::format("{}\"{}\":{}", c ? "," : "", OpenFailures[c].key, OpenFailures[c].value(s));
            }
            out += "}";
            for (auto const& single : Singles)
            {
                out += fmt::format(",\"{}\":{}", single.key, single.value(s));
            }
            out += flow.lastErrorAt ? fmt::format(",\"lastError\":{{\"message\":{},\"secondsAgo\":{:.1f}}}}}", jsonString(flow.lastError),
                                          (now - flow.lastErrorAt) / 1e9)
                                    : std::string{",\"lastError\":null}"};
        }
        return out + "]}";
    }

    double ms(std::int64_t ns)
    {
        return static_cast<double>(ns) / 1e6;
    }

    // One batch for the ring view: per flow, what it is drawn against and the
    // events since the cursor. An event is [index, seen ms, arrival ms,
    // commit ms or null, behind head, kind, count].
    std::string streamBatch(std::vector<std::unique_ptr<FlowStats>> const& flows, std::vector<std::uint64_t>& cursors)
    {
        auto out = fmt::format("{{\"now\":{:.3f},\"flows\":[", ms(static_cast<std::int64_t>(mxlGetTime())));
        for (std::size_t i = 0; i < flows.size(); ++i)
        {
            auto const& flow = *flows[i];
            auto const lock = std::lock_guard{flow.mutex};
            out += fmt::format("{}{{\"id\":{},\"flowId\":{},\"label\":{},\"unit\":\"{}\",\"state\":\"{}\",\"ring\":{},\"slotBytes\":{},"
                               "\"periodMs\":{:.4f},\"graceMs\":{:.3f},\"reader\":{},\"head\":{},\"ev\":[",
                i ? "," : "", jsonString(flow.id), jsonString(flow.target), jsonString(flow.format), flow.continuous ? "samples" : "grains",
                mxlMonitoringAccessName(mxlMonitoringGetFlowAccess(flow.monitor)), flow.ringLength.load(), flow.slotBytes.load(), ms(static_cast<std::int64_t>(flow.periodNs.load())),
                ms(static_cast<std::int64_t>(flow.graceNs.load())), flow.readerIndex.load(), flow.headSeen.load());
            auto first = true;
            for (auto const& e : flow.events)
            {
                if (e.seq <= cursors[i])
                {
                    continue;
                }
                out += fmt::format("{}[{},{:.3f},{:.3f},{},{},\"{}\",{}]", first ? "" : ",", e.index, ms(static_cast<std::int64_t>(e.seenNs)),
                    ms(e.arrivalNs), e.commitNs == GrainEvent::Unknown ? std::string{"null"} : fmt::format("{:.3f}", ms(e.commitNs)),
                    e.behind, e.kind, e.count);
                first = false;
            }
            cursors[i] = flow.eventSeq;
            out += "]}";
        }
        return out + "]}";
    }

    // Short, so one client that sends nothing or reads slowly cannot hold a
    // probe past its own timeout: requests are answered one at a time, and
    // reading and answering each get this long.
    constexpr auto RequestBudget = std::chrono::milliseconds{200};
    // A viewer that takes longer than this to take one batch is dropped.
    constexpr auto StreamSendBudget = std::chrono::seconds{2};

    std::atomic<int> streams{0};
    // Each open ring view costs a thread and a batch every 50 ms.
    constexpr int MaxStreams = 8;

    // Server-sent events for the ring view, 20 batches a second, on a thread
    // of its own so a viewer never holds up the probes or /status. A new viewer
    // gets the last five seconds first, so the view has something to draw.
    void streamClient(int client, std::vector<std::unique_ptr<FlowStats>> const& flows)
    {
        auto const header = std::string{"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\n"
                                        "Connection: keep-alive\r\nX-Accel-Buffering: no\r\n\r\n"};
        auto sent = reader::sendAll(client, header, StreamSendBudget);
        auto cursors = std::vector<std::uint64_t>(flows.size(), 0);
        auto const since = mxlGetTime() - 5 * NsPerSecond;
        for (std::size_t i = 0; i < flows.size(); ++i)
        {
            auto const lock = std::lock_guard{flows[i]->mutex};
            for (auto const& e : flows[i]->events)
            {
                if (e.seenNs < since)
                {
                    cursors[i] = e.seq;
                }
            }
        }
        while (sent && !stopping)
        {
            auto const message = "data: " + streamBatch(flows, cursors) + "\n\n";
            sent = reader::sendAll(client, message, StreamSendBudget);
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        ::close(client);
        --streams;
    }

    char const* reason(int code)
    {
        switch (code)
        {
            case 200: return "OK";
            case 400: return "Bad Request";
            case 404: return "Not Found";
            case 405: return "Method Not Allowed";
            case 501: return "Not Implemented";
            case 503: return "Service Unavailable";
        }
        return "Internal Server Error";
    }

    void serve(int port, std::vector<std::unique_ptr<FlowStats>> const& flows, mxlMonitoring monitoring, reader::nmos::Node* node)
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
            auto const request = reader::parseRequest(reader::readRequest(client, RequestBudget));
            // The NMOS APIs take GET and PATCH; everything else here is read.
            if (node != nullptr && request.path.starts_with("/x-nmos/"))
            {
                auto const reply = node->handle(request.method, request.path, request.body);
                auto const response = fmt::format("HTTP/1.1 {} {}\r\nContent-Type: application/json\r\n"
                                                  "Content-Length: {}\r\nConnection: close\r\n\r\n{}",
                    reply.code, reason(reply.code), reply.body.size(), reply.body);
                reader::sendAll(client, response, RequestBudget);
                ::close(client);
                continue;
            }
            auto const path = request.method == "GET" ? request.path : std::string{};

            if (path == "/stream")
            {
                if (streams.fetch_add(1) < MaxStreams)
                {
                    std::thread{streamClient, client, std::cref(flows)}.detach();
                    continue;
                }
                --streams;
            }

            auto code = 200;
            auto type = std::string{"text/plain; version=0.0.4"};
            std::string body;
            if (path == "/")
            {
                body = RingPage;
                type = "text/html; charset=utf-8";
            }
            else if (path == "/stream")
            {
                code = 503;
                body = fmt::format("at most {} ring views at once\n", MaxStreams);
            }
            else if (path == "/metrics")
            {
                body = metrics(monitoring);
            }
            else if (path == "/status")
            {
                body = status(flows, monitoring);
                type = "application/json";
            }
            else if (path == "/delivering")
            {
                // A slot no flow is connected to reads nothing on purpose.
                auto all = true;
                for (auto const& s : snapshots(monitoring))
                {
                    all = all && (s.access == MXL_MONITORING_ACCESS_DISCONNECTED || s.delivering);
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
                code, reason(code), type, body.size(), body);
            reader::sendAll(client, response, RequestBudget);
            ::close(client);
        }
        ::close(fd);
    }


    // The id of the MXL domain, from the domain_def.json BCP-007-03 has every
    // domain carry; empty when it cannot be read.
    std::string domainId(std::string const& domain)
    {
        auto* file = std::fopen((domain + "/domain_def.json").c_str(), "r");
        if (file == nullptr)
        {
            return {};
        }
        auto text = std::string{};
        char buffer[4096];
        while (auto const n = std::fread(buffer, 1, sizeof(buffer), file))
        {
            text.append(buffer, n);
        }
        std::fclose(file);
        auto json = picojson::value{};
        if (!picojson::parse(json, text).empty() || !json.is<picojson::object>() || !json.get("id").is<std::string>())
        {
            return {};
        }
        return json.get("id").get<std::string>();
    }

    // Registers the node with an IS-04 Registration API and keeps it
    // registered: a heartbeat every five seconds, registration again when
    // the registry has dropped the node, and a receiver posted again when its
    // subscription changes.
    void registerNode(reader::nmos::Node& node, std::string const& host, int port)
    {
        constexpr auto Budget = std::chrono::seconds{5};
        constexpr auto Resource = "/x-nmos/registration/v1.3/resource";
        auto const post = [&](std::string const& type, std::string const& data) {
            auto const r = reader::request(host, port, "POST", Resource, fmt::format("{{\"type\":\"{}\",\"data\":{}}}", type, data), Budget);
            if (r.code != 200 && r.code != 201)
            {
                std::fprintf(stderr, "nmos: registering a %s failed: %d %s\n", type.c_str(), r.code, r.body.substr(0, 300).c_str());
                return false;
            }
            return true;
        };
        auto registered = false;
        while (!stopping)
        {
            // A fixed period, whatever the requests took, so a slow registry
            // does not stretch the heartbeat towards its expiry.
            auto const due = std::chrono::steady_clock::now() + std::chrono::seconds{5};
            if (!registered)
            {
                node.takeChanged();
                registered = true;
                for (auto const& [type, data] : node.resources())
                {
                    if (!post(type, data))
                    {
                        registered = false;
                        break;
                    }
                }
                if (registered)
                {
                    std::fprintf(stderr, "nmos: node %s registered with %s:%d\n", node.nodeId().c_str(), host.c_str(), port);
                }
            }
            else
            {
                // A receiver update the registry missed is sent again with a
                // full registration, or the registry keeps the old state.
                for (auto const& data : node.takeChanged())
                {
                    if (!post("receiver", data))
                    {
                        registered = false;
                    }
                }
                if (!registered)
                {
                    continue;
                }
                auto const r = reader::request(host, port, "POST", "/x-nmos/registration/v1.3/health/nodes/" + node.nodeId(), "", Budget);
                if (r.code == 404)
                {
                    std::fprintf(stderr, "nmos: the registry dropped the node, registering again\n");
                    registered = false;
                    continue;
                }
                if (r.code != 200)
                {
                    std::fprintf(stderr, "nmos: heartbeat failed: %d\n", r.code);
                }
            }
            while (!stopping && std::chrono::steady_clock::now() < due)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds{100});
            }
        }
        if (registered)
        {
            reader::request(host, port, "DELETE", std::string{Resource} + "/nodes/" + node.nodeId(), "", std::chrono::seconds{2});
        }
    }

}

int main(int argc, char** argv)
{
    auto app = CLI::App{"mxl-reader"};
    app.set_version_flag("--version", versionText());
    auto domain = std::string{};
    app.add_option("-d,--domain", domain, "The MXL domain directory.")->required();
    auto flowSpecs = std::vector<std::string>{};
    app.add_option("-f,--flow", flowSpecs,
           "A flow id to read, or a named slot <name>:<video|audio|data>[=<flow id>] an NMOS controller can connect. "
           "Repeat for several.")
        ->required();
    auto expect = std::string{};
    app.add_option("--expect", expect, "JSON object of what each flow should carry, keyed by flow id or slot name.");
    auto port = 9102;
    app.add_option("-p,--port", port, "Port of the metrics, status and NMOS endpoints.");
    auto reportSeconds = 10;
    app.add_option("--report-interval", reportSeconds, "Seconds between summary lines on stderr. 0 disables them.");
    auto nmosSeed = std::string{};
    app.add_option("--nmos-seed", nmosSeed,
        "Serve an NMOS Node whose named slots are MXL Receivers, with resource ids derived from this seed. "
        "Keep it across restarts.");
    auto nmosRegistry = std::string{};
    app.add_option("--nmos-registry", nmosRegistry, "IS-04 registry to register the node with, as http://<host>[:<port>].");
    auto nmosHost = std::string{};
    app.add_option("--nmos-host", nmosHost, "Host the registry and controllers reach this node at. Empty: the hostname.");
    auto nmosLabel = std::string{"mxl-reader"};
    app.add_option("--nmos-label", nmosLabel, "Label of the NMOS node and device.");
    CLI11_PARSE(app, argc, argv);

    // libmxl answers a malformed id as an invalid argument, which the read loop
    // would take for a flow not there yet and wait on for ever.
    auto slots = std::vector<reader::nmos::Slot>{};
    auto keys = std::vector<std::string>{};
    for (auto const& spec : flowSpecs)
    {
        auto slot = reader::nmos::Slot{};
        if (auto const err = reader::nmos::parseSlot(spec, slot); !err.empty())
        {
            std::fprintf(stderr, "%s\n", err.c_str());
            return 2;
        }
        if (std::find(keys.begin(), keys.end(), slot.key) != keys.end())
        {
            std::fprintf(stderr, "read twice: %s\n", slot.key.c_str());
            return 2;
        }
        // Only a controller connects a slot booked without a flow, and only
        // a slot with a kind can be a Receiver.
        if (slot.name.empty() && !nmosSeed.empty())
        {
            std::fprintf(stderr, "with --nmos-seed every flow is a named slot <name>:<kind>[=<flow id>]: %s\n", spec.c_str());
            return 2;
        }
        if (slot.flow.empty() && nmosSeed.empty())
        {
            std::fprintf(stderr, "slot %s has no flow and no NMOS controller can connect one without --nmos-seed\n", slot.key.c_str());
            return 2;
        }
        keys.push_back(slot.key);
        slots.push_back(std::move(slot));
    }
    auto registryHost = std::string{};
    auto registryPort = 80;
    if (!nmosRegistry.empty())
    {
        static auto const re = std::regex{"^http://([^/:]+)(:([0-9]{1,5}))?/?$"};
        auto m = std::smatch{};
        if (nmosSeed.empty() || !std::regex_match(nmosRegistry, m, re))
        {
            std::fprintf(stderr, "--nmos-registry takes http://<host>[:<port>] and needs --nmos-seed\n");
            return 2;
        }
        registryHost = m[1].str();
        if (m[3].matched)
        {
            registryPort = std::stoi(m[3].str());
            if (registryPort < 1 || registryPort > 65535)
            {
                std::fprintf(stderr, "--nmos-registry port out of range: %d\n", registryPort);
                return 2;
            }
        }
    }
    auto expectations = std::map<std::string, Expectation>{};
    if (auto const err = reader::parseExpectations(expect, keys, expectations); !err.empty())
    {
        std::fprintf(stderr, "%s\n", err.c_str());
        return 2;
    }
    for (auto const& [key, e] : expectations)
    {
        if (!e.rate.empty() && !reader::nmos::parseRate(e.rate))
        {
            std::fprintf(stderr, "--expect: the rate of %s must be <numerator>/<denominator>: %s\n", key.c_str(), e.rate.c_str());
            return 2;
        }
    }

    std::signal(SIGTERM, onSignal);
    std::signal(SIGINT, onSignal);

    std::fprintf(stderr, "%s, reading %zu flow%s from %s, serving on :%d\n", versionText().c_str(), slots.size(),
        slots.size() == 1 ? "" : "s", domain.c_str(), port);

    auto monitoring = mxlMonitoring{};
    if (mxlMonitoringCreate(nullptr, &monitoring) != MXL_STATUS_OK)
    {
        std::fprintf(stderr, "cannot set up monitoring\n");
        return 1;
    }
    auto flows = std::vector<std::unique_ptr<FlowStats>>{};
    for (auto const& slot : slots)
    {
        flows.push_back(std::make_unique<FlowStats>());
        auto& stats = *flows.back();
        stats.id = slot.key;
        stats.target = slot.flow;
        if (auto const status = mxlMonitoringAddFlow(monitoring, slot.key.c_str(), MXL_MONITORING_ROLE_READER, &stats.monitor);
            status != MXL_STATUS_OK)
        {
            std::fprintf(stderr, "cannot monitor %s: status %d\n", slot.key.c_str(), static_cast<int>(status));
            return 1;
        }
        // Before any thread records; the reader thread retargets it later.
        mxlMonitoringSetFlowTarget(stats.monitor, slot.flow.c_str());
        if (auto const it = expectations.find(slot.key); it != expectations.end())
        {
            stats.expected = it->second;
            auto const expected = reader::toMonitoring(it->second);
            mxlMonitoringSetExpectation(stats.monitor, &expected);
        }
    }

    auto node = std::unique_ptr<reader::nmos::Node>{};
    if (!nmosSeed.empty())
    {
        if (nmosHost.empty())
        {
            char name[256] = {};
            ::gethostname(name, sizeof(name) - 1);
            nmosHost = name;
        }
        auto const domainIdValue = domainId(domain);
        if (domainIdValue.empty())
        {
            std::fprintf(stderr, "nmos: %s/domain_def.json names no domain id; a connection that names a domain is refused\n",
                domain.c_str());
        }
        node = std::make_unique<reader::nmos::Node>(
            reader::nmos::NodeConfig{.seed = nmosSeed, .label = nmosLabel, .description = "mxl-reader", .host = nmosHost,
                .port = port, .domainId = domainIdValue},
            [&flows](std::size_t slot, std::string const& flow) {
                retarget(*flows.at(slot), flow);
                return std::string{};
            });
        for (std::size_t i = 0; i < slots.size(); ++i)
        {
            auto constraints = std::optional<reader::nmos::Constraints>{};
            if (auto const& e = flows[i]->expected)
            {
                constraints = reader::nmos::Constraints{.width = e->width, .height = e->height, .rate = e->rate,
                    .sampleRate = e->sampleRate, .channels = e->channels};
            }
            node->addReceiver(slots[i].name, slots[i].kind, constraints, slots[i].flow);
        }
        std::fprintf(stderr, "nmos: node %s with %zu receiver%s at %s:%d\n", node->nodeId().c_str(), slots.size(),
            slots.size() == 1 ? "" : "s", nmosHost.c_str(), port);
    }

    auto threads = std::vector<std::thread>{};
    for (auto& flow : flows)
    {
        threads.emplace_back(readFlow, domain, std::ref(*flow));
    }
    threads.emplace_back(serve, port, std::cref(flows), monitoring, node.get());
    if (node && !registryHost.empty())
    {
        threads.emplace_back(registerNode, std::ref(*node), registryHost, registryPort);
    }

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
        auto const counts = snapshots(monitoring);
        for (std::size_t i = 0; i < flows.size() && i < counts.size(); ++i)
        {
            auto const& s = counts[i];
            auto const ok = s.reads[MXL_MONITORING_READ_OK];
            auto const wrong = s.content[MXL_MONITORING_CONTENT_WRONG_SIZE] + s.content[MXL_MONITORING_CONTENT_WRONG_FORMAT] +
                               s.content[MXL_MONITORING_CONTENT_WRONG_RESOLUTION] + s.content[MXL_MONITORING_CONTENT_WRONG_RATE];
            std::fprintf(stderr,
                "flow %s: %s, %.1f %s/s ok, early %llu, late %llu, invalid %llu, wrong %llu, illegal %llu, stalls %llu, age %.3f s\n",
                flows[i]->id.c_str(), s.delivering ? "delivering" : mxlMonitoringAccessName(s.access),
                static_cast<double>(ok - previous[i]) / reportSeconds, flows[i]->continuous ? "samples" : "grains",
                static_cast<unsigned long long>(s.reads[MXL_MONITORING_READ_TOO_EARLY]),
                static_cast<unsigned long long>(s.reads[MXL_MONITORING_READ_TOO_LATE]),
                static_cast<unsigned long long>(s.content[MXL_MONITORING_CONTENT_INVALID]), static_cast<unsigned long long>(wrong),
                static_cast<unsigned long long>(s.content[MXL_MONITORING_CONTENT_ILLEGAL_VALUES]), static_cast<unsigned long long>(s.stalls),
                static_cast<double>(s.ageNs) / 1e9);
            previous[i] = ok;
        }
    }

    for (auto& thread : threads)
    {
        thread.join();
    }
    // Ring views run on detached threads that read the monitoring; each
    // sees stopping within a batch, and is given a second to.
    for (auto waited = 0; streams > 0 && waited < 20; ++waited)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds{50});
    }
    if (streams == 0)
    {
        mxlMonitoringDestroy(monitoring);
    }
    return failed ? 1 : 0;
}
