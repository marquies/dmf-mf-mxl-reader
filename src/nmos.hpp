// NMOS for mxl-reader: an IS-04 Node whose MXL readers are Receivers, and the
// IS-05 Connection API a controller connects them to flows through, as AMWA
// BCP-007-03 specifies for MXL. Nothing here touches libmxl or a socket, so it
// is tested on its own; mxl-reader.cpp serves it and applies what it decides.

#pragma once

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <map>
#include <mutex>
#include <optional>
#include <regex>
#include <string>
#include <string_view>
#include <vector>

#include <fmt/format.h>
#include <picojson/wrapper.h>

namespace reader::nmos
{
    constexpr char const* TransportMXL = "urn:x-nmos:transport:mxl";
    constexpr char const* GroupHintTag = "urn:x-nmos:tag:grouphint/v1.0";
    constexpr char const* NodeBase = "/x-nmos/node/v1.3/";
    constexpr char const* ConnectionBase = "/x-nmos/connection/v1.1/";

    // SHA-1, for name-based UUIDs only (RFC 4122 version 5), not for security.
    inline std::array<std::uint8_t, 20> sha1(std::string_view data)
    {
        std::uint32_t h[5] = {0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476, 0xC3D2E1F0};
        auto message = std::string{data};
        auto const bits = static_cast<std::uint64_t>(data.size()) * 8;
        message.push_back(static_cast<char>(0x80));
        while (message.size() % 64 != 56)
        {
            message.push_back(0);
        }
        for (int i = 7; i >= 0; --i)
        {
            message.push_back(static_cast<char>((bits >> (i * 8)) & 0xff));
        }
        auto const rotl = [](std::uint32_t v, int n) { return (v << n) | (v >> (32 - n)); };
        for (std::size_t chunk = 0; chunk < message.size(); chunk += 64)
        {
            std::uint32_t w[80];
            for (int i = 0; i < 16; ++i)
            {
                auto const* p = reinterpret_cast<unsigned char const*>(message.data() + chunk + i * 4);
                w[i] = (std::uint32_t{p[0]} << 24) | (std::uint32_t{p[1]} << 16) | (std::uint32_t{p[2]} << 8) | p[3];
            }
            for (int i = 16; i < 80; ++i)
            {
                w[i] = rotl(w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16], 1);
            }
            auto a = h[0], b = h[1], c = h[2], d = h[3], e = h[4];
            for (int i = 0; i < 80; ++i)
            {
                std::uint32_t f, k;
                if (i < 20)
                {
                    f = (b & c) | (~b & d), k = 0x5A827999;
                }
                else if (i < 40)
                {
                    f = b ^ c ^ d, k = 0x6ED9EBA1;
                }
                else if (i < 60)
                {
                    f = (b & c) | (b & d) | (c & d), k = 0x8F1BBCDC;
                }
                else
                {
                    f = b ^ c ^ d, k = 0xCA62C1D6;
                }
                auto const t = rotl(a, 5) + f + e + k + w[i];
                e = d, d = c, c = rotl(b, 30), b = a, a = t;
            }
            h[0] += a, h[1] += b, h[2] += c, h[3] += d, h[4] += e;
        }
        auto out = std::array<std::uint8_t, 20>{};
        for (int i = 0; i < 5; ++i)
        {
            for (int j = 0; j < 4; ++j)
            {
                out[i * 4 + j] = static_cast<std::uint8_t>((h[i] >> (24 - j * 8)) & 0xff);
            }
        }
        return out;
    }

    // A fixed namespace, so ids derived here do not collide with ids derived
    // anywhere else from the same names.
    constexpr std::array<std::uint8_t, 16> IdNamespace = {
        0x4e, 0x1f, 0x7a, 0x23, 0x9c, 0x61, 0x4b, 0x0d, 0xa5, 0x3e, 0x12, 0x88, 0xc7, 0x5f, 0x30, 0x9b};

    // The stable id of one resource: a version 5 UUID over the seed and the
    // parts. The same seed gives the same ids after every restart, which is
    // what lets a controller re-establish the connections it made.
    inline std::string deriveId(std::string const& seed, std::initializer_list<std::string_view> parts)
    {
        auto input = std::string(IdNamespace.begin(), IdNamespace.end()) + seed;
        for (auto const part : parts)
        {
            // A separator no name contains keeps ("a", "bc") from ("ab", "c").
            input.push_back('\0');
            input.append(part);
        }
        auto b = sha1(input);
        b[6] = static_cast<std::uint8_t>((b[6] & 0x0f) | 0x50);
        b[8] = static_cast<std::uint8_t>((b[8] & 0x3f) | 0x80);
        return fmt::format("{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]);
    }

    inline bool isUuid(std::string const& s)
    {
        static auto const re = std::regex{"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"};
        return std::regex_match(s, re);
    }

    // An IS-04 version: TAI seconds and nanoseconds.
    inline std::string version()
    {
        auto const ns = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        return fmt::format("{}:{:09}", ns / 1'000'000'000 + 37, ns % 1'000'000'000);
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

    inline char const* formatUrn(Kind kind)
    {
        switch (kind)
        {
            case Kind::Video: return "urn:x-nmos:format:video";
            case Kind::Audio: return "urn:x-nmos:format:audio";
            case Kind::Data:  return "urn:x-nmos:format:data";
        }
        return "urn:x-nmos:format:video";
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
    // controller can move to other flows and may leave without a flow.
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

    inline picojson::value str(std::string const& s)
    {
        return picojson::value{s};
    }

    inline picojson::value num(double v)
    {
        return picojson::value{v};
    }

    inline picojson::value enumOf(picojson::value v)
    {
        auto o = picojson::object{};
        o["enum"] = picojson::value{picojson::array{std::move(v)}};
        return picojson::value{o};
    }

    inline picojson::value nullable(std::optional<std::string> const& s)
    {
        return s ? str(*s) : picojson::value{};
    }

    // The staged or active state of an MXL Receiver.
    struct Endpoint
    {
        std::optional<std::string> senderId;
        bool masterEnable = false;
        std::optional<std::string> mode;
        std::optional<std::string> requestedTime;
        std::optional<std::string> activationTime;
        std::optional<std::string> domainId;
        std::optional<std::string> flowId;

        // Every attribute IS-05 requires of a receiver, null where unset.
        picojson::value json() const
        {
            auto activation = picojson::object{};
            activation["mode"] = nullable(mode);
            activation["requested_time"] = nullable(requestedTime);
            activation["activation_time"] = nullable(activationTime);
            auto file = picojson::object{};
            file["data"] = picojson::value{};
            file["type"] = picojson::value{};
            auto leg = picojson::object{};
            leg["mxl_domain_id"] = nullable(domainId);
            leg["mxl_flow_id"] = nullable(flowId);
            auto o = picojson::object{};
            o["sender_id"] = nullable(senderId);
            o["master_enable"] = picojson::value{masterEnable};
            o["activation"] = picojson::value{activation};
            o["transport_file"] = picojson::value{file};
            o["transport_params"] = picojson::value{picojson::array{picojson::value{leg}}};
            return picojson::value{o};
        }
    };

    // A rejected or answered request: the status and a JSON body.
    struct Reply
    {
        int code = 200;
        std::string body;
    };

    inline Reply error(int code, std::string const& debug)
    {
        auto const text = code == 400 ? "Bad Request" : code == 404 ? "Not Found" : code == 405 ? "Method Not Allowed"
                        : code == 501 ? "Not Implemented" : "Internal Server Error";
        auto o = picojson::object{};
        o["code"] = num(code);
        o["error"] = str(text);
        o["debug"] = str(debug);
        return {code, picojson::value{o}.serialize()};
    }

    // What a PATCH to a receiver's staged endpoint asks for: the new staged
    // state and, for an immediate activation, the state to make active.
    struct Staging
    {
        Reply reply;
        Endpoint staged;
        std::optional<Endpoint> activate;
    };

    // Parses null, a lowercase UUID, or "auto" where allowed.
    inline std::optional<std::string> nullableId(picojson::value const& v, char const* name, bool allowAuto, std::string& err)
    {
        if (v.is<picojson::null>())
        {
            return std::nullopt;
        }
        if (!v.is<std::string>())
        {
            err = fmt::format("{} must be a string or null", name);
            return std::nullopt;
        }
        auto const& s = v.get<std::string>();
        if (s == "auto")
        {
            if (!allowAuto)
            {
                err = fmt::format("{} does not accept auto", name);
            }
            return s;
        }
        if (!isUuid(s))
        {
            err = fmt::format("{} {} is not a lowercase UUID", name, s);
            return std::nullopt;
        }
        return s;
    }

    // Applies a PATCH body to a receiver's staged state as BCP-007-03 has an
    // MXL Receiver do: one leg of mxl_domain_id and mxl_flow_id, "auto" only
    // for the domain and resolved to this node's, a domain the node cannot
    // reach refused, no transport file, and immediate activations only.
    inline Staging stage(Endpoint staged, std::string const& body, std::string const& domainId)
    {
        auto json = picojson::value{};
        if (auto const err = picojson::parse(json, body); !err.empty() || !json.is<picojson::object>())
        {
            return {error(400, "the body must be a JSON object")};
        }
        auto const& o = json.get<picojson::object>();
        static std::vector<std::string> const known = {"sender_id", "master_enable", "activation", "transport_params", "transport_file"};
        for (auto const& [key, _] : o)
        {
            if (std::find(known.begin(), known.end(), key) == known.end())
            {
                return {error(400, "unknown attribute " + key)};
            }
        }
        auto err = std::string{};
        if (auto const it = o.find("sender_id"); it != o.end())
        {
            staged.senderId = nullableId(it->second, "sender_id", false, err);
        }
        if (auto const it = o.find("master_enable"); it != o.end())
        {
            if (!it->second.is<bool>())
            {
                return {error(400, "master_enable must be a boolean")};
            }
            staged.masterEnable = it->second.get<bool>();
        }
        if (auto const it = o.find("transport_file"); it != o.end() && !it->second.is<picojson::null>())
        {
            auto const ok = it->second.is<picojson::object>() && it->second.get("data").is<picojson::null>() &&
                            it->second.get("type").is<picojson::null>();
            if (!ok)
            {
                return {error(400, "an MXL receiver takes no transport file; data and type must be null")};
            }
        }
        if (auto const it = o.find("transport_params"); it != o.end())
        {
            if (!it->second.is<picojson::array>() || it->second.get<picojson::array>().size() != 1 ||
                !it->second.get<picojson::array>()[0].is<picojson::object>())
            {
                return {error(400, "MXL takes exactly one set of transport parameters")};
            }
            auto const& leg = it->second.get<picojson::array>()[0].get<picojson::object>();
            for (auto const& [key, value] : leg)
            {
                if (key == "mxl_domain_id")
                {
                    staged.domainId = nullableId(value, "mxl_domain_id", true, err);
                    if (err.empty() && staged.domainId && *staged.domainId != "auto" && *staged.domainId != domainId)
                    {
                        err = "mxl_domain_id " + *staged.domainId + " is not the MXL domain this node reads";
                    }
                }
                else if (key == "mxl_flow_id")
                {
                    staged.flowId = nullableId(value, "mxl_flow_id", false, err);
                }
                else
                {
                    err = "unknown MXL transport parameter " + key;
                }
                if (!err.empty())
                {
                    break;
                }
            }
        }
        if (!err.empty())
        {
            return {error(400, err)};
        }
        auto activate = false;
        if (auto const it = o.find("activation"); it != o.end())
        {
            if (!it->second.is<picojson::object>())
            {
                return {error(400, "activation must be an object")};
            }
            auto const& mode = it->second.get("mode");
            auto const& requested = it->second.get("requested_time");
            staged.requestedTime = requested.is<std::string>() ? std::optional{requested.get<std::string>()} : std::nullopt;
            if (mode.is<std::string>())
            {
                auto const& m = mode.get<std::string>();
                if (m == "activate_scheduled_absolute" || m == "activate_scheduled_relative")
                {
                    return {error(501, "activation mode " + m + " is not implemented")};
                }
                if (m != "activate_immediate")
                {
                    return {error(400, "unknown activation mode " + m)};
                }
                staged.mode = m;
                activate = true;
            }
            else if (!mode.is<picojson::null>())
            {
                return {error(400, "activation mode must be a string or null")};
            }
            else
            {
                staged.mode.reset();
            }
        }
        if (!activate)
        {
            return {Reply{200, staged.json().serialize()}, staged};
        }

        auto active = staged;
        if (active.domainId == "auto")
        {
            // A disconnect reads nothing and needs no domain.
            if (domainId.empty() && active.masterEnable)
            {
                return {error(500, "mxl_domain_id auto: no MXL domain is known to this node")};
            }
            active.domainId = domainId.empty() ? std::nullopt : std::optional{domainId};
        }
        if (active.masterEnable && !active.flowId)
        {
            return {error(400, "master_enable needs an mxl_flow_id to read")};
        }
        // An enabled receiver reads in the node's one domain, and active
        // says so.
        if (active.masterEnable && !active.domainId && !domainId.empty())
        {
            active.domainId = domainId;
        }
        active.activationTime = version();
        // Once the transaction is done the staged activation returns to null;
        // the reply carries what was activated.
        auto reply = staged;
        reply.activationTime = active.activationTime;
        staged.mode.reset();
        staged.requestedTime.reset();
        return {Reply{200, reply.json().serialize()}, staged, active};
    }

    // One Receiver of the node.
    struct Receiver
    {
        std::string id;
        std::string name;
        Kind kind = Kind::Video;
        std::optional<Constraints> constraints;
        std::string version;
        std::string capsVersion;
        Endpoint staged;
        Endpoint active;
    };

    struct NodeConfig
    {
        std::string seed;
        std::string label;
        std::string description;
        std::string host;
        int port = 0;
        // The MXL domain's id from its domain_def.json; empty when unknown.
        std::string domainId;
    };

    // Moves a slot to a flow, or off it for an empty one. An error refuses
    // the activation.
    using ApplyFn = std::function<std::string(std::size_t slot, std::string const& flow)>;

    // The node: its resources, served on the Node API, and its Receivers'
    // connections, served on the Connection API.
    class Node
    {
    public:
        Node(NodeConfig config, ApplyFn apply)
            : config_{std::move(config)}, apply_{std::move(apply)}, nodeId_{deriveId(config_.seed, {"node"})},
              deviceId_{deriveId(config_.seed, {"device"})}, version_{nmos::version()}
        {
        }

        // A Receiver for a named slot, connected to flow when one is given.
        void addReceiver(std::string const& name, Kind kind, std::optional<Constraints> constraints, std::string const& flow)
        {
            auto r = Receiver{.id = deriveId(config_.seed, {"receiver", name, kindName(kind)}), .name = name, .kind = kind,
                .constraints = std::move(constraints), .version = nmos::version(), .capsVersion = nmos::version()};
            if (!flow.empty())
            {
                r.active.masterEnable = true;
                r.active.flowId = flow;
                if (!config_.domainId.empty())
                {
                    r.active.domainId = config_.domainId;
                }
                r.staged = r.active;
            }
            receivers_.push_back(std::move(r));
        }

        std::string const& nodeId() const { return nodeId_; }

        std::string receiverId(std::size_t slot) const { return receivers_.at(slot).id; }

        // The resources a registry is given, in the order it needs them:
        // node, device, then each receiver.
        std::vector<std::pair<std::string, std::string>> resources() const
        {
            auto const lock = std::lock_guard{mutex_};
            auto out = std::vector<std::pair<std::string, std::string>>{{"node", nodeJson().serialize()}, {"device", deviceJson().serialize()}};
            for (auto const& r : receivers_)
            {
                out.emplace_back("receiver", receiverJson(r).serialize());
            }
            return out;
        }

        // Receivers whose subscription changed since the last call, for the
        // registry to be told.
        std::vector<std::string> takeChanged()
        {
            auto const lock = std::lock_guard{mutex_};
            auto out = std::vector<std::string>{};
            for (auto const index : changed_)
            {
                out.push_back(receiverJson(receivers_[index]).serialize());
            }
            changed_.clear();
            return out;
        }

        // Answers one request on the Node or Connection API.
        Reply handle(std::string const& method, std::string const& path, std::string const& body)
        {
            auto const lock = std::lock_guard{mutex_};
            auto const root = path.substr(0, path.find_last_not_of('/') + 1);
            if (root == "/x-nmos" || root == "/x-nmos/node" || root == "/x-nmos/connection")
            {
                if (method != "GET")
                {
                    return error(405, "read-only");
                }
                return root == "/x-nmos" ? list({"node/", "connection/"}) : root == "/x-nmos/node" ? list({"v1.3/"}) : list({"v1.1/"});
            }
            if (path.starts_with(NodeBase) || path + "/" == NodeBase)
            {
                return method == "GET" ? nodeApi(path.size() >= std::string_view{NodeBase}.size() ? path.substr(std::string_view{NodeBase}.size()) : "")
                                       : error(405, "the Node API is read-only");
            }
            if (path.starts_with(ConnectionBase) || path + "/" == ConnectionBase)
            {
                return connectionApi(method, path.size() >= std::string_view{ConnectionBase}.size() ? path.substr(std::string_view{ConnectionBase}.size()) : "", body);
            }
            return error(404, "not an NMOS API path");
        }

    private:
        static Reply list(std::vector<std::string> const& items)
        {
            auto a = picojson::array{};
            for (auto const& i : items)
            {
                a.push_back(str(i));
            }
            return {200, picojson::value{a}.serialize()};
        }

        Reply nodeApi(std::string const& rest) const
        {
            auto const sub = rest.substr(0, rest.find_last_not_of('/') + 1);
            if (sub.empty())
            {
                return list({"self/", "devices/", "sources/", "flows/", "senders/", "receivers/"});
            }
            if (sub == "self")
            {
                return {200, nodeJson().serialize()};
            }
            if (sub == "sources" || sub == "flows" || sub == "senders")
            {
                return {200, "[]"};
            }
            if (sub == "devices")
            {
                return {200, picojson::value{picojson::array{deviceJson()}}.serialize()};
            }
            if (sub == "devices/" + deviceId_)
            {
                return {200, deviceJson().serialize()};
            }
            if (sub == "receivers")
            {
                auto a = picojson::array{};
                for (auto const& r : receivers_)
                {
                    a.push_back(receiverJson(r));
                }
                return {200, picojson::value{a}.serialize()};
            }
            for (auto const& r : receivers_)
            {
                if (sub == "receivers/" + r.id)
                {
                    return {200, receiverJson(r).serialize()};
                }
            }
            return error(404, "no such resource");
        }

        Reply connectionApi(std::string const& method, std::string const& rest, std::string const& body)
        {
            auto sub = rest.substr(0, rest.find_last_not_of('/') + 1);
            auto const get = [&](Reply reply) { return method == "GET" ? reply : error(405, "read-only"); };
            if (sub.empty())
            {
                return get(list({"single/"}));
            }
            if (sub == "single")
            {
                return get(list({"senders/", "receivers/"}));
            }
            if (sub == "single/senders")
            {
                return get({200, "[]"});
            }
            if (sub == "single/receivers")
            {
                auto ids = std::vector<std::string>{};
                for (auto const& r : receivers_)
                {
                    ids.push_back(r.id + "/");
                }
                return get(list(ids));
            }
            for (std::size_t i = 0; i < receivers_.size(); ++i)
            {
                auto& r = receivers_[i];
                auto const base = "single/receivers/" + r.id;
                if (sub == base)
                {
                    return get(list({"constraints/", "staged/", "active/", "transporttype/"}));
                }
                if (sub == base + "/transporttype")
                {
                    return get({200, str(TransportMXL).serialize()});
                }
                if (sub == base + "/constraints")
                {
                    auto leg = picojson::object{};
                    leg["mxl_domain_id"] = config_.domainId.empty() ? picojson::value{picojson::object{}} : enumOf(str(config_.domainId));
                    leg["mxl_flow_id"] = picojson::value{picojson::object{}};
                    return get({200, picojson::value{picojson::array{picojson::value{leg}}}.serialize()});
                }
                if (sub == base + "/active")
                {
                    return get({200, r.active.json().serialize()});
                }
                if (sub == base + "/staged")
                {
                    if (method == "GET")
                    {
                        return {200, r.staged.json().serialize()};
                    }
                    if (method != "PATCH")
                    {
                        return error(405, "staged takes GET and PATCH");
                    }
                    return patch(i, body);
                }
            }
            return error(404, "no such resource");
        }

        Reply patch(std::size_t index, std::string const& body)
        {
            auto& r = receivers_[index];
            auto s = stage(r.staged, body, config_.domainId);
            if (s.reply.code != 200)
            {
                return s.reply;
            }
            if (s.activate)
            {
                auto const flow = s.activate->masterEnable ? s.activate->flowId.value_or("") : std::string{};
                if (auto const err = apply_(index, flow); !err.empty())
                {
                    return error(500, err);
                }
                auto const subscribed = s.activate->masterEnable;
                if (r.active.senderId != s.activate->senderId || r.active.masterEnable != subscribed)
                {
                    r.version = nmos::version();
                    changed_.push_back(index);
                }
                r.active = *s.activate;
            }
            r.staged = s.staged;
            return s.reply;
        }

        picojson::value nodeJson() const
        {
            auto endpoint = picojson::object{};
            endpoint["host"] = str(config_.host);
            endpoint["port"] = num(config_.port);
            endpoint["protocol"] = str("http");
            auto api = picojson::object{};
            api["versions"] = picojson::value{picojson::array{str("v1.3")}};
            api["endpoints"] = picojson::value{picojson::array{picojson::value{endpoint}}};
            auto clock = picojson::object{};
            clock["name"] = str("clk0");
            clock["ref_type"] = str("internal");
            auto o = picojson::object{};
            o["id"] = str(nodeId_);
            o["version"] = str(version_);
            o["label"] = str(config_.label);
            o["description"] = str(config_.description);
            o["tags"] = picojson::value{picojson::object{}};
            o["href"] = str(fmt::format("http://{}:{}/", config_.host, config_.port));
            o["hostname"] = str(config_.host);
            o["api"] = picojson::value{api};
            o["caps"] = picojson::value{picojson::object{}};
            o["services"] = picojson::value{picojson::array{}};
            o["clocks"] = picojson::value{picojson::array{picojson::value{clock}}};
            o["interfaces"] = picojson::value{picojson::array{}};
            return picojson::value{o};
        }

        picojson::value deviceJson() const
        {
            auto receivers = picojson::array{};
            for (auto const& r : receivers_)
            {
                receivers.push_back(str(r.id));
            }
            auto control = picojson::object{};
            control["type"] = str("urn:x-nmos:control:sr-ctrl/v1.1");
            control["href"] = str(fmt::format("http://{}:{}{}", config_.host, config_.port, ConnectionBase));
            auto o = picojson::object{};
            o["id"] = str(deviceId_);
            o["version"] = str(version_);
            o["label"] = str(config_.label);
            o["description"] = str(config_.description);
            o["tags"] = picojson::value{picojson::object{}};
            o["type"] = str("urn:x-nmos:device:generic");
            o["node_id"] = str(nodeId_);
            o["senders"] = picojson::value{picojson::array{}};
            o["receivers"] = picojson::value{receivers};
            o["controls"] = picojson::value{picojson::array{picojson::value{control}}};
            return picojson::value{o};
        }

        // BCP-007-03 has an MXL Receiver state its media types and, per
        // BCP-004-01, what it can consume as constraint sets.
        picojson::value receiverJson(Receiver const& r) const
        {
            auto set = picojson::object{};
            set["urn:x-nmos:cap:format:media_type"] = enumOf(str(mediaType(r.kind)));
            if (auto const& c = r.constraints)
            {
                if (c->width)
                {
                    set["urn:x-nmos:cap:format:frame_width"] = enumOf(num(c->width));
                }
                if (c->height)
                {
                    set["urn:x-nmos:cap:format:frame_height"] = enumOf(num(c->height));
                }
                if (auto const rate = parseRate(c->rate))
                {
                    auto r = picojson::object{};
                    r["numerator"] = num(static_cast<double>(rate->first));
                    r["denominator"] = num(static_cast<double>(rate->second));
                    set["urn:x-nmos:cap:format:grain_rate"] = enumOf(picojson::value{r});
                }
                if (c->sampleRate)
                {
                    auto rate = picojson::object{};
                    rate["numerator"] = num(c->sampleRate);
                    set["urn:x-nmos:cap:format:sample_rate"] = enumOf(picojson::value{rate});
                }
                if (c->channels)
                {
                    set["urn:x-nmos:cap:format:channel_count"] = enumOf(num(c->channels));
                }
            }
            auto caps = picojson::object{};
            caps["media_types"] = picojson::value{picojson::array{str(mediaType(r.kind))}};
            caps["constraint_sets"] = picojson::value{picojson::array{picojson::value{set}}};
            caps["version"] = str(r.capsVersion);
            auto subscription = picojson::object{};
            subscription["sender_id"] = nullable(r.active.senderId);
            subscription["active"] = picojson::value{r.active.masterEnable};
            auto tags = picojson::object{};
            tags[GroupHintTag] = picojson::value{picojson::array{str(fmt::format("{}:{}", r.name, groupRole(r.kind)))}};
            auto o = picojson::object{};
            o["id"] = str(r.id);
            o["version"] = str(r.version);
            o["label"] = str(fmt::format("{} {}", r.name, kindName(r.kind)));
            o["description"] = str(fmt::format("MXL reader {}, {}", r.name, kindName(r.kind)));
            o["tags"] = picojson::value{tags};
            o["format"] = str(formatUrn(r.kind));
            o["caps"] = picojson::value{caps};
            o["transport"] = str(TransportMXL);
            o["device_id"] = str(deviceId_);
            o["interface_bindings"] = picojson::value{picojson::array{}};
            o["subscription"] = picojson::value{subscription};
            return picojson::value{o};
        }

        NodeConfig config_;
        ApplyFn apply_;
        std::string nodeId_;
        std::string deviceId_;
        std::string version_;
        std::vector<Receiver> receivers_;
        std::vector<std::size_t> changed_;
        mutable std::mutex mutex_;
    };
}
