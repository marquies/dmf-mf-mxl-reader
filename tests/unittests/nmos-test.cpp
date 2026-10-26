// Tests of mxl-reader's NMOS Node: the resources it registers, the IS-05
// Connection API it serves for its MXL Receivers as BCP-007-03 specifies, and
// the request handling both need. Built and run by the image build.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdio>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <picojson/wrapper.h>

#include "http.hpp"
#include "nmos.hpp"

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

    using namespace reader::nmos;

    constexpr char const* Domain = "1ac254d9-a5eb-475f-a2b6-3d02a5cfbc82";
    constexpr char const* Flow = "4c02e7c0-ffeb-4595-8673-a265d103123a";
    constexpr char const* Other = "3494dc19-22c2-4f00-865b-cdbeee7909f0";
    constexpr char const* Sender = "f68987c7-80fa-4b72-b299-00e66e1b1a41";

    picojson::value parse(std::string const& text)
    {
        auto v = picojson::value{};
        picojson::parse(v, text);
        return v;
    }

    // What a test node did to its slots.
    struct Applied
    {
        std::vector<std::pair<std::size_t, std::string>> calls;
        std::string refuse;
    };

    std::unique_ptr<Node> node(Applied& applied, std::string const& domain = Domain)
    {
        auto n = std::make_unique<Node>(NodeConfig{.seed = "p-show/reader", .label = "reader", .host = "reader.p-show.svc", .port = 9102, .domainId = domain},
            [&applied](std::size_t slot, std::string const& flow) {
                if (!applied.refuse.empty())
                {
                    return applied.refuse;
                }
                applied.calls.emplace_back(slot, flow);
                return std::string{};
            });
        n->addReceiver("cam1", Kind::Video, Constraints{.width = 1920, .height = 1080, .rate = "50/1"}, Flow);
        n->addReceiver("cam2", Kind::Audio, std::nullopt, "");
        return n;
    }

    std::string staged(Node const& n, std::size_t slot)
    {
        return std::string{ConnectionBase} + "single/receivers/" + n.receiverId(slot) + "/staged";
    }

    // Name-based ids: stable for a seed, different across seeds and names,
    // and legal IS-04 ids; the SHA-1 they rest on is the standard one.
    void ids()
    {
        auto const digest = sha1("abc");
        check(digest[0] == 0xa9 && digest[1] == 0x99 && digest[19] == 0x9d, "SHA-1 of abc is the standard digest");
        auto const two = sha1("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq");
        check(two[0] == 0x84 && two[1] == 0x98 && two[18] == 0x70 && two[19] == 0xf1, "SHA-1 over two blocks is the standard digest");
        auto const a = deriveId("p-show/reader", {"receiver", "cam1"});
        check(a == deriveId("p-show/reader", {"receiver", "cam1"}), "an id is the same on every restart");
        check(a != deriveId("p-other/reader", {"receiver", "cam1"}), "another seed gives another id");
        check(a != deriveId("p-show/reader", {"receiver", "cam1", ""}) && deriveId("s", {"a", "bc"}) != deriveId("s", {"ab", "c"}),
            "parts are kept apart");
        check(isUuid(a) && a[14] == '5', "an id is a version 5 UUID");
    }

    void rates()
    {
        check(parseRate("50/1") == std::pair{50LL, 1LL} && parseRate("30000/1001") == std::pair{30000LL, 1001LL}, "a rate is read");
        for (auto const bad : {"x/1", "/1", "50", "50/0", "50/1x", ""})
        {
            check(!parseRate(bad), "a rate that is no rational is refused, not thrown on");
        }
    }

    void slots()
    {
        auto s = Slot{};
        check(parseSlot(Flow, s).empty() && s.key == Flow && s.flow == Flow && s.name.empty(), "a flow id is keyed by the id");
        check(parseSlot(std::string{"cam1:video="} + Flow, s).empty() && s.key == "cam1:video" && s.name == "cam1" &&
                  s.kind == Kind::Video && s.flow == Flow,
            "a named slot is keyed by its name and kind, so one name can hold video and audio");
        check(parseSlot("cam2:audio", s).empty() && s.flow.empty() && s.kind == Kind::Audio, "a named slot may come without a flow");
        check(!parseSlot("Cam1:video", s).empty(), "a name is a lowercase DNS label");
        check(!parseSlot("cam1:anc", s).empty(), "a kind is video, audio or data");
        check(!parseSlot("cam1:video=nope", s).empty(), "a named slot's flow is a flow id");
    }

    // BCP-007-03: a Receiver per MXL reader, on the MXL transport with no
    // interface bindings, stating media types and BCP-004-01 constraint sets
    // with a version, grouped by group hint.
    void resources()
    {
        auto applied = Applied{};
        auto const n = node(applied);
        auto const res = n->resources();
        check(res.size() == 4 && res[0].first == "node" && res[1].first == "device" && res[2].first == "receiver",
            "a registry gets node, device, then receivers");
        auto const rx = parse(res[2].second);
        check(rx.get("transport").to_str() == TransportMXL, "a receiver is on the MXL transport");
        check(rx.get("interface_bindings").is<picojson::array>() && rx.get("interface_bindings").get<picojson::array>().empty(),
            "an MXL receiver binds no interface");
        check(rx.get("format").to_str() == "urn:x-nmos:format:video", "a video slot is a video receiver");
        auto const& caps = rx.get("caps");
        check(caps.get("media_types").get<picojson::array>()[0].to_str() == "video/v210", "it states the media type it reads");
        auto const& set = caps.get("constraint_sets").get<picojson::array>()[0];
        check(set.get("urn:x-nmos:cap:format:frame_width").get("enum").get<picojson::array>()[0].get<double>() == 1920,
            "what the booking expects narrows the capabilities");
        check(set.get("urn:x-nmos:cap:format:grain_rate").get("enum").get<picojson::array>()[0].get("denominator").get<double>() == 1,
            "a rate is a rational");
        check(caps.get("version").is<std::string>(), "constraint sets carry a version");
        check(rx.get("tags").get(GroupHintTag).get<picojson::array>()[0].to_str() == "cam1:Video", "a receiver is grouped by its slot");
        check(rx.get("subscription").get("active").get<bool>(), "a slot booked with a flow is subscribed");
        auto const audio = parse(res[3].second);
        check(audio.get("format").to_str() == "urn:x-nmos:format:audio" && !audio.get("subscription").get("active").get<bool>(),
            "a slot booked without a flow is not subscribed");
        auto const device = parse(res[1].second);
        check(device.get("receivers").get<picojson::array>().size() == 2 &&
                  device.get("controls").get<picojson::array>()[0].get("href").to_str() == "http://reader.p-show.svc:9102/x-nmos/connection/v1.1/",
            "the device lists its receivers and where they are connected");
    }

    void unconfigured()
    {
        auto applied = Applied{};
        auto n = node(applied);
        auto const active = parse(n->handle("GET", std::string{ConnectionBase} + "single/receivers/" + n->receiverId(1) + "/active", "").body);
        for (auto const key : {"sender_id", "master_enable", "activation", "transport_file", "transport_params"})
        {
            check(active.contains(key), "an endpoint carries every attribute IS-05 requires");
        }
        auto const& leg = active.get("transport_params").get<picojson::array>()[0];
        check(leg.contains("mxl_flow_id") && leg.get("mxl_flow_id").is<picojson::null>() && leg.get("mxl_domain_id").is<picojson::null>(),
            "an unconfigured receiver holds both parameters, null");
        auto const cons = parse(n->handle("GET", std::string{ConnectionBase} + "single/receivers/" + n->receiverId(1) + "/constraints", "").body);
        check(cons.get<picojson::array>().size() == 1 &&
                  cons.get<picojson::array>()[0].get("mxl_domain_id").get("enum").get<picojson::array>()[0].to_str() == Domain,
            "constraints name this node's domain and never auto");
        check(n->handle("GET", std::string{ConnectionBase} + "single/receivers/" + n->receiverId(0) + "/transporttype", "").body ==
                  "\"urn:x-nmos:transport:mxl\"",
            "the transport type is MXL");
    }

    // A controller connects a slot: auto resolves to the domain, the slot's
    // reader moves to the flow, and the node publishes the new subscription.
    void connect()
    {
        auto applied = Applied{};
        auto n = node(applied);
        auto const reply = n->handle("PATCH", staged(*n, 1),
            std::string{R"({"sender_id":")"} + Sender + R"(","master_enable":true,"activation":{"mode":"activate_immediate"},)" +
                R"("transport_params":[{"mxl_domain_id":"auto","mxl_flow_id":")" + Other + R"("}]})");
        check(reply.code == 200, "an immediate activation is accepted");
        check(applied.calls.size() == 1 && applied.calls[0].first == 1 && applied.calls[0].second == Other, "the slot moves to the flow");
        check(!parse(reply.body).get("activation").get("activation_time").is<picojson::null>(), "the reply says when it was activated");
        auto const active = parse(n->handle("GET", std::string{ConnectionBase} + "single/receivers/" + n->receiverId(1) + "/active", "").body);
        check(active.get("transport_params").get<picojson::array>()[0].get("mxl_domain_id").to_str() == Domain,
            "auto is resolved in what is active");
        check(parse(n->handle("GET", staged(*n, 1), "").body).get("activation").get("mode").is<picojson::null>(),
            "the staged activation returns to null");
        auto const changed = n->takeChanged();
        check(changed.size() == 1 && parse(changed[0]).get("subscription").get("sender_id").to_str() == Sender,
            "the receiver's new subscription is handed to the registry");
        check(n->takeChanged().empty(), "a change is handed over once");

        // Disconnecting reads nothing.
        check(n->handle("PATCH", staged(*n, 1), R"({"master_enable":false,"activation":{"mode":"activate_immediate"}})").code == 200,
            "a disconnect is accepted");
        check(applied.calls.size() == 2 && applied.calls[1].second.empty(), "a disconnect moves the slot off its flow");
    }

    void stagingOnly()
    {
        auto applied = Applied{};
        auto n = node(applied);
        auto const code = n->handle("PATCH", staged(*n, 1), std::string{R"({"master_enable":true,"transport_params":[{"mxl_flow_id":")"} + Flow + R"("}]})").code;
        check(code == 200 && applied.calls.empty(), "staging alone changes nothing that runs");
    }

    void rejections()
    {
        struct Case
        {
            char const* what;
            std::string body;
            int code;
        };
        auto const cases = std::vector<Case>{
            {"mxl_flow_id does not accept auto", R"({"transport_params":[{"mxl_flow_id":"auto"}]})", 400},
            {"a domain the node cannot reach is refused", R"({"transport_params":[{"mxl_domain_id":"3310f209-9351-47c0-b9a2-14c59b6a4c23"}]})", 400},
            {"a flow id is a UUID", R"({"transport_params":[{"mxl_flow_id":"cam1"}]})", 400},
            {"MXL takes one leg", R"({"transport_params":[{},{}]})", 400},
            {"RTP parameters are unknown here", R"({"transport_params":[{"source_ip":"10.0.0.1"}]})", 400},
            {"a transport file is refused", R"({"transport_file":{"data":"v=0","type":"application/sdp"}})", 400},
            {"scheduled activations are not implemented", R"({"activation":{"mode":"activate_scheduled_relative","requested_time":"1:0"}})", 501},
            {"enabling a receiver without a flow is refused", R"({"master_enable":true,"activation":{"mode":"activate_immediate"}})", 400},
            {"the body is JSON", "nope", 400},
        };
        for (auto const& c : cases)
        {
            auto applied = Applied{};
            auto n = node(applied);
            auto const reply = n->handle("PATCH", staged(*n, 1), c.body);
            check(reply.code == c.code && applied.calls.empty(), c.what);
        }
        auto applied = Applied{};
        auto n = node(applied);
        check(n->handle("PATCH", staged(*n, 1), R"({"transport_file":{"data":null,"type":null}})").code == 200,
            "a null transport file means none and is accepted");
        check(n->handle("PATCH", std::string{NodeBase} + "self", "{}").code == 405, "the Node API is read-only");
        check(n->handle("GET", std::string{ConnectionBase} + "single/receivers/nope/staged", "").code == 404, "an unknown receiver is not found");
    }

    // An activation the reader cannot apply leaves what is active as it was.
    void refused()
    {
        auto applied = Applied{.refuse = "no"};
        auto n = node(applied);
        auto const code = n->handle("PATCH", staged(*n, 1),
            std::string{R"({"master_enable":true,"activation":{"mode":"activate_immediate"},"transport_params":[{"mxl_flow_id":")"} + Flow + R"("}]})").code;
        check(code == 500, "a refused activation fails");
        auto const active = parse(n->handle("GET", std::string{ConnectionBase} + "single/receivers/" + n->receiverId(1) + "/active", "").body);
        check(!active.get("master_enable").get<bool>(), "a refused activation leaves active as it was");
    }

    // The version lists above each API, as IS-04 and IS-05 serve them.
    void roots()
    {
        auto applied = Applied{};
        auto n = node(applied);
        auto const first = [&](std::string const& path) {
            auto const v = parse(n->handle("GET", path, "").body);
            return v.is<picojson::array>() && !v.get<picojson::array>().empty() ? v.get<picojson::array>()[0].to_str() : std::string{};
        };
        check(first("/x-nmos/") == "node/", "the NMOS root lists the APIs");
        check(first("/x-nmos/node/") == "v1.3/", "the Node API root lists its version");
        check(first("/x-nmos/connection") == "v1.1/", "the Connection API root lists its version");
    }

    // With no domain id known, auto cannot resolve, and a domain cannot be
    // named, but a connection naming only the flow still works.
    void unknownDomain()
    {
        auto applied = Applied{};
        auto n = node(applied, "");
        check(n->handle("PATCH", staged(*n, 1),
                  std::string{R"({"master_enable":true,"activation":{"mode":"activate_immediate"},"transport_params":[{"mxl_domain_id":"auto","mxl_flow_id":")"} + Flow + R"("}]})")
                      .code == 500,
            "auto with no known domain fails the activation");
        check(n->handle("PATCH", staged(*n, 1),
                  std::string{R"({"master_enable":true,"activation":{"mode":"activate_immediate"},"transport_params":[{"mxl_domain_id":null,"mxl_flow_id":")"} + Flow + R"("}]})")
                      .code == 200,
            "a connection naming only the flow works without a domain id");
        check(n->handle("PATCH", staged(*n, 1),
                  R"({"master_enable":false,"activation":{"mode":"activate_immediate"},"transport_params":[{"mxl_domain_id":"auto"}]})")
                      .code == 200,
            "a disconnect needs no domain, auto or not");
    }

    // A PATCH carries a body after its headers; the request is read through it.
    void requestBodies()
    {
        auto const raw = std::string{"PATCH /x-nmos/connection/v1.1/single/receivers/x/staged?y=1 HTTP/1.1\r\nHost: r\r\nContent-Length: 2\r\n\r\n{}"};
        auto const r = reader::parseRequest(raw);
        check(r.method == "PATCH" && r.path == "/x-nmos/connection/v1.1/single/receivers/x/staged" && r.body == "{}",
            "a request's method, path and body are taken apart");
        check(reader::header("X\r\ncontent-LENGTH:  12 \r\n", "Content-Length") == "12", "a header is found without regard to case");

        int fds[2];
        check(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0, "a socket pair opens");
        auto const head = std::string{"PATCH /s HTTP/1.1\r\nContent-Length: 11\r\n\r\n"};
        auto received = std::string{};
        auto server = std::thread{[&] { received = reader::readRequest(fds[0], std::chrono::seconds{2}); }};
        ::send(fds[1], head.data(), head.size(), MSG_NOSIGNAL);
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        ::send(fds[1], "{\"a\":\"bcd\"}", 11, MSG_NOSIGNAL);
        server.join();
        ::close(fds[0]);
        ::close(fds[1]);
        check(received == head + "{\"a\":\"bcd\"}", "a body that arrives after the headers is read too");

        // A length that is no number must not take the reader down.
        check(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0, "a socket pair opens");
        auto const bad = std::string{"PATCH /s HTTP/1.1\r\nContent-Length: lots\r\n\r\n"};
        ::send(fds[1], bad.data(), bad.size(), MSG_NOSIGNAL);
        check(reader::readRequest(fds[0], std::chrono::milliseconds{200}) == bad, "a malformed length reads as no body");
        ::close(fds[0]);
        ::close(fds[1]);
    }

    // The registry client: a request goes out whole and the status and body
    // come back.
    void client()
    {
        auto const listener = ::socket(AF_INET, SOCK_STREAM, 0);
        auto addr = sockaddr_in{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        auto len = socklen_t{sizeof(addr)};
        if (listener < 0 || ::bind(listener, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 || ::listen(listener, 1) != 0 ||
            ::getsockname(listener, reinterpret_cast<sockaddr*>(&addr), &len) != 0)
        {
            check(false, "a loopback listener opens");
            return;
        }
        auto got = reader::Request{};
        auto server = std::thread{[&] {
            auto const fd = ::accept(listener, nullptr, nullptr);
            got = reader::parseRequest(reader::readRequest(fd, std::chrono::seconds{2}));
            reader::sendAll(fd, "HTTP/1.1 201 Created\r\nContent-Length: 2\r\n\r\nok", std::chrono::seconds{2});
            ::close(fd);
        }};
        auto const r = reader::request("127.0.0.1", ntohs(addr.sin_port), "POST", "/x-nmos/registration/v1.3/resource", R"({"type":"node"})",
            std::chrono::seconds{2});
        server.join();
        ::close(listener);
        check(got.method == "POST" && got.path == "/x-nmos/registration/v1.3/resource" && got.body == R"({"type":"node"})",
            "the registry receives the whole request");
        check(r.code == 201 && r.body == "ok", "the registry's status and body come back");
        check(reader::request("127.0.0.1", 1, "GET", "/", "", std::chrono::milliseconds{200}).code == 0, "an unreachable registry gives code 0");
    }
}

int main()
{
    ids();
    rates();
    slots();
    resources();
    unconfigured();
    connect();
    stagingOnly();
    rejections();
    refused();
    roots();
    unknownDomain();
    requestBodies();
    client();
    if (failures == 0)
    {
        std::fprintf(stderr, "all NMOS checks pass\n");
    }
    return failures == 0 ? 0 : 1;
}
