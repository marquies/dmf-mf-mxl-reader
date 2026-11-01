// NMOS for mxl-reader, the part that decides rather than serves: the slots a
// controller connects, the MXL flow definition each slot's Receiver is created
// from, what an activation asks a slot to read, and what each Receiver's AMWA
// BCP-008 monitor reports. libnvnmos serves the Node, IS-05 and IS-12;
// nothing here touches it, a socket or libmxl, so it is tested on its own.

#pragma once

#include <algorithm>
#include <array>
#include <charconv>
#include <cstdint>
#include <optional>
#include <regex>
#include <string>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <mxl-monitoring/bcp008.h>
#include <mxl-monitoring/monitoring.h>
#include <picojson/wrapper.h>

namespace reader::nmos
{
    constexpr char const* GroupHintTag = "urn:x-nmos:tag:grouphint/v1.0";
    // libnvnmos's extensions to an MXL flow definition.
    constexpr char const* NameTag = "urn:x-nvnmos:tag:name";
    constexpr char const* DomainTag = "urn:x-nvnmos:tag:mxl-domain-id";
    constexpr char const* CapsTag = "urn:x-nvnmos:tag:caps";
    // What a flow definition names when there is no flow; libnvnmos takes the
    // flow a Receiver reads from IS-05, not from here.
    constexpr char const* NoFlow = "00000000-0000-0000-0000-000000000000";

    inline bool isUuid(std::string const& s)
    {
        static auto const re = std::regex{"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"};
        return std::regex_match(s, re);
    }

    // What one reader slot reads, which decides the Receiver's format.
    enum class Kind
    {
        Video,
        Audio,
        Data,
    };

    inline char const* kindName(Kind kind)
    {
        switch (kind)
        {
            case Kind::Video: return "video";
            case Kind::Audio: return "audio";
            case Kind::Data:  return "data";
        }
        return "video";
    }

    inline char const* mediaType(Kind kind)
    {
        switch (kind)
        {
            case Kind::Video: return "video/v210";
            case Kind::Audio: return "audio/float32";
            case Kind::Data:  return "video/smpte291";
        }
        return "video/v210";
    }

    // The role a group hint names, as other MXL nodes spell it.
    inline char const* groupRole(Kind kind)
    {
        switch (kind)
        {
            case Kind::Video: return "Video";
            case Kind::Audio: return "Audio";
            case Kind::Data:  return "Data";
        }
        return "Video";
    }

    // One --flow argument: a flow id, keyed by that id, or a named slot
    // "<name>:<kind>[=<flow id>]", keyed by "<name>:<kind>", which an NMOS
    // controller can move to other flows and may leave without a flow. The
    // key is also the name of the slot's Receiver in libnvnmos, from which
    // the Receiver's id derives.
    struct Slot
    {
        std::string key;
        std::string name;
        Kind kind = Kind::Video;
        std::string flow;
    };

    inline std::string parseSlot(std::string const& spec, Slot& out)
    {
        if (isUuid(spec))
        {
            out = Slot{.key = spec, .flow = spec};
            return {};
        }
        static auto const re = std::regex{"^([a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?):(video|audio|data)(=(.*))?$"};
        auto m = std::smatch{};
        if (!std::regex_match(spec, m, re))
        {
            return "not a lowercase flow UUID or <name>:<video|audio|data>[=<flow id>]: " + spec;
        }
        auto const kind = m[3].str();
        // Keyed by name and kind, so one name can group a video, an audio and
        // a data receiver, as a group hint does.
        out = Slot{.key = m[1].str() + ":" + kind, .name = m[1].str()};
        out.kind = kind == "audio" ? Kind::Audio : kind == "data" ? Kind::Data : Kind::Video;
        out.flow = m[5].str();
        if (!out.flow.empty() && !isUuid(out.flow))
        {
            return "not a lowercase flow UUID: " + out.flow;
        }
        return {};
    }

    // A rate "<numerator>/<denominator>" with both positive, or none.
    inline std::optional<std::pair<long long, long long>> parseRate(std::string const& rate)
    {
        auto const slash = rate.find('/');
        if (slash == std::string::npos)
        {
            return std::nullopt;
        }
        auto n = 0LL, d = 0LL;
        auto const [pn, en] = std::from_chars(rate.data(), rate.data() + slash, n);
        auto const [pd, ed] = std::from_chars(rate.data() + slash + 1, rate.data() + rate.size(), d);
        if (en != std::errc{} || ed != std::errc{} || pn != rate.data() + slash || pd != rate.data() + rate.size() || n <= 0 || d <= 0)
        {
            return std::nullopt;
        }
        return std::pair{n, d};
    }

    // What a booking says a slot should carry, for the Receiver's BCP-004-01
    // capabilities: a controller offers only flows that fit.
    struct Constraints
    {
        int width = 0;
        int height = 0;
        std::string rate;
        int sampleRate = 0;
        int channels = 0;
    };

    inline picojson::value strings(std::vector<std::string> const& values)
    {
        auto a = picojson::array{};
        for (auto const& v : values)
        {
            a.emplace_back(v);
        }
        return picojson::value{a};
    }

    inline picojson::value rational(long long numerator, long long denominator)
    {
        auto o = picojson::object{};
        o["numerator"] = picojson::value{static_cast<double>(numerator)};
        o["denominator"] = picojson::value{static_cast<double>(denominator)};
        return picojson::value{o};
    }

    inline picojson::value component(char const* name, int width, int height)
    {
        auto o = picojson::object{};
        o["name"] = picojson::value{name};
        o["width"] = picojson::value{static_cast<double>(width)};
        o["height"] = picojson::value{static_cast<double>(height)};
        o["bit_depth"] = picojson::value{10.0};
        return picojson::value{o};
    }

    // The MXL flow definition libnvnmos creates a slot's Receiver from, or,
    // with a flow, reports the slot connected to it by. libnvnmos derives the
    // Receiver's capabilities from the definition's format, all of them or
    // none: it needs the geometry and rate of a video flow and the sample
    // rate and channels of an audio flow, so a slot whose --expect leaves any
    // of them open is created without format constraints rather than with
    // made-up ones. v210 is 10-bit 4:2:2 and float32 32-bit, which no
    // --expect needs to say. An empty domainId leaves the domain to the
    // controller.
    inline std::string flowDefinition(Slot const& slot, std::optional<Constraints> const& constraints, std::string const& domainId,
        std::string const& flow)
    {
        auto tags = picojson::object{};
        tags[NameTag] = strings({slot.key});
        tags[GroupHintTag] = strings({fmt::format("{}:{}", slot.name, groupRole(slot.kind))});
        if (!domainId.empty())
        {
            tags[DomainTag] = strings({domainId});
        }
        auto o = picojson::object{};
        o["id"] = picojson::value{flow.empty() ? std::string{NoFlow} : flow};
        o["label"] = picojson::value{fmt::format("{} {}", slot.name, kindName(slot.kind))};
        o["description"] = picojson::value{fmt::format("MXL reader {}, {}", slot.name, kindName(slot.kind))};
        o["media_type"] = picojson::value{mediaType(slot.kind)};
        auto const c = constraints.value_or(Constraints{});
        auto const rate = parseRate(c.rate);
        auto caps = false;
        switch (slot.kind)
        {
            case Kind::Video:
                o["format"] = picojson::value{"urn:x-nmos:format:video"};
                caps = c.width > 0 && c.height > 0 && rate;
                if (caps)
                {
                    o["grain_rate"] = rational(rate->first, rate->second);
                    o["frame_width"] = picojson::value{static_cast<double>(c.width)};
                    o["frame_height"] = picojson::value{static_cast<double>(c.height)};
                    o["components"] = picojson::value{picojson::array{component("Y", c.width, c.height), component("Cb", c.width / 2, c.height),
                        component("Cr", c.width / 2, c.height)}};
                }
                break;
            case Kind::Audio:
                o["format"] = picojson::value{"urn:x-nmos:format:audio"};
                caps = c.sampleRate > 0 && c.channels > 0;
                if (caps)
                {
                    o["sample_rate"] = rational(c.sampleRate, 1);
                    o["channel_count"] = picojson::value{static_cast<double>(c.channels)};
                    o["bit_depth"] = picojson::value{32.0};
                }
                break;
            case Kind::Data:
                o["format"] = picojson::value{"urn:x-nmos:format:data"};
                caps = rate.has_value();
                if (caps)
                {
                    o["grain_rate"] = rational(rate->first, rate->second);
                }
                break;
        }
        if (!caps)
        {
            // Present at all, libnvnmos leaves out the format constraints.
            tags[CapsTag] = strings({"none"});
        }
        o["tags"] = picojson::value{tags};
        return picojson::value{o}.serialize();
    }

    // What an activation asks of a slot: the flow to read, empty for none.
    // libnvnmos hands a Receiver's activation over as its flow definition
    // with the IS-05 mxl_flow_id as "id" and mxl_domain_id in the domain tag,
    // or no definition for a deactivation, or for master_enable with a null
    // flow, which leaves nothing to read. IS-05 refuses a domain other than
    // the node's when the node knows its domain id; when it does not, a
    // domain named is one the reader cannot tell is its own, and error says
    // so.
    inline std::string connectedFlow(char const* transportFile, std::string const& domainId, std::string& error)
    {
        error.clear();
        if (transportFile == nullptr)
        {
            return {};
        }
        auto json = picojson::value{};
        if (!picojson::parse(json, transportFile).empty() || !json.is<picojson::object>() || !json.get("id").is<std::string>() ||
            !isUuid(json.get("id").get<std::string>()))
        {
            error = "the activation names no flow id";
            return {};
        }
        auto domain = std::string{};
        if (auto const& tag = json.get("tags").get(DomainTag); tag.is<picojson::array>() && !tag.get<picojson::array>().empty() &&
                                                                tag.get<picojson::array>()[0].is<std::string>())
        {
            domain = tag.get<picojson::array>()[0].get<std::string>();
        }
        if (!domain.empty() && domain != domainId)
        {
            error = fmt::format("mxl_domain_id {} is not the MXL domain this node reads", domain);
            return {};
        }
        return json.get("id").get<std::string>();
    }

    // The statuses a Receiver's monitor is handed for one poll, in the order
    // link, connection, synchronization, stream; MXL_MONITORING_BCP008_UNCHANGED
    // for a domain to leave as it is. previous is the slot's snapshot of the
    // poll before, which counts nothing towards a slot moved to another flow
    // since: what the old flow did is not the new one's health.
    inline std::array<mxlMonitoringBcp008Domain, 4> judge(std::optional<mxlMonitoringFlowSnapshot> const& previous,
        mxlMonitoringFlowSnapshot const& current)
    {
        auto statuses = mxlMonitoringBcp008Statuses{};
        statuses.version = 1;
        statuses.size = sizeof(statuses);
        auto const* const before = previous && std::string{previous->target} == current.target ? &*previous : nullptr;
        if (mxlMonitoringBcp008GetStatuses(before, &current, &statuses) != MXL_STATUS_OK)
        {
            auto const unchanged = mxlMonitoringBcp008Domain{.status = MXL_MONITORING_BCP008_UNCHANGED, .message = {}};
            return {unchanged, unchanged, unchanged, unchanged};
        }
        return {statuses.link, statuses.connection, statuses.synchronization, statuses.stream};
    }

    // A monitor's counters of one kind, counted since baseline, the slot's
    // snapshot at the monitor's last reset, or since start without one.
    inline std::vector<mxlMonitoringBcp008Counter> counters(std::optional<mxlMonitoringFlowSnapshot> const& baseline,
        mxlMonitoringFlowSnapshot const& latest, mxlMonitoringBcp008Counters which)
    {
        auto out = std::vector<mxlMonitoringBcp008Counter>(4);
        auto count = out.size();
        if (mxlMonitoringBcp008GetCounters(baseline ? &*baseline : nullptr, &latest, which, out.data(), &count) != MXL_STATUS_OK)
        {
            return {};
        }
        out.resize(std::min(count, out.size()));
        return out;
    }
}
