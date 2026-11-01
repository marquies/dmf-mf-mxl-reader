// Tests of what mxl-reader's NMOS Node decides: the slots a controller
// connects, the MXL flow definition each Receiver is created from, what an
// activation asks a slot to read, and what each Receiver's BCP-008 monitor
// reports. libnvnmos serves them; the integration test reaches it. Built and
// run by the image build.

#include <cstdio>
#include <cstring>
#include <string>

#include <picojson/wrapper.h>

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

    picojson::value parse(std::string const& text)
    {
        auto v = picojson::value{};
        picojson::parse(v, text);
        return v;
    }

    Slot slot(std::string const& spec)
    {
        auto s = Slot{};
        parseSlot(spec, s);
        return s;
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

    std::string tag(picojson::value const& def, char const* name)
    {
        auto const& t = def.get("tags").get(name);
        return t.is<picojson::array>() && !t.get<picojson::array>().empty() ? t.get<picojson::array>()[0].to_str() : std::string{};
    }

    // libnvnmos names the Receiver, groups it and derives its capabilities
    // from the definition: all of them from a complete one, none otherwise.
    void definitions()
    {
        auto const video = parse(flowDefinition(slot("cam1:video"), Constraints{.width = 1920, .height = 1080, .rate = "50/1"}, Domain, ""));
        check(tag(video, NameTag) == "cam1:video", "a Receiver is named by its slot's key, which /status shows");
        check(tag(video, GroupHintTag) == "cam1:Video", "a Receiver is grouped by its slot");
        check(tag(video, DomainTag) == Domain, "a Receiver is constrained to the domain the reader reads");
        check(video.get("id").to_str() == NoFlow, "a slot without a flow names none");
        check(video.get("media_type").to_str() == "video/v210" && video.get("label").to_str() == "cam1 video",
            "a video slot reads v210 and is labelled by name and kind");
        check(video.get("frame_width").get<double>() == 1920 && video.get("grain_rate").get("numerator").get<double>() == 50 &&
                  video.get("components").get<picojson::array>().size() == 3 &&
                  video.get("components").get<picojson::array>()[1].get("width").get<double>() == 960,
            "a complete video expectation gives the geometry, rate and 4:2:2 sampling to constrain");
        check(!video.get("tags").contains(CapsTag), "a complete expectation keeps the format constraints");

        auto const partial = parse(flowDefinition(slot("cam1:video"), Constraints{.width = 1920, .height = 1080}, Domain, ""));
        check(tag(partial, CapsTag) != "" && !partial.contains("frame_width"),
            "a video expectation without a rate constrains nothing rather than a made-up rate");
        auto const none = parse(flowDefinition(slot("cam1:video"), std::nullopt, "", Flow));
        check(tag(none, CapsTag) != "" && !none.get("tags").contains(DomainTag), "no expectation and no domain id constrain nothing");
        check(none.get("id").to_str() == Flow, "a definition reporting a connection names the flow");

        auto const audio = parse(flowDefinition(slot("mic:audio"), Constraints{.sampleRate = 48000, .channels = 2}, Domain, ""));
        check(audio.get("media_type").to_str() == "audio/float32" && audio.get("sample_rate").get("numerator").get<double>() == 48000 &&
                  audio.get("channel_count").get<double>() == 2 && audio.get("bit_depth").get<double>() == 32 &&
                  !audio.get("tags").contains(CapsTag) && tag(audio, GroupHintTag) == "mic:Audio",
            "a complete audio expectation gives sample rate, channels and the float depth");
        auto const audioPartial = parse(flowDefinition(slot("mic:audio"), Constraints{.sampleRate = 48000}, Domain, ""));
        check(tag(audioPartial, CapsTag) != "", "an audio expectation without channels constrains nothing");

        auto const data = parse(flowDefinition(slot("anc:data"), Constraints{.rate = "25/1"}, Domain, ""));
        check(data.get("media_type").to_str() == "video/smpte291" && data.get("grain_rate").get("numerator").get<double>() == 25 &&
                  !data.get("tags").contains(CapsTag),
            "a data slot is constrained by its rate");
    }

    std::string activation(std::string const& flow, std::string const& domain)
    {
        auto def = parse(flowDefinition(slot("cam1:video"), std::nullopt, "", flow));
        auto tags = def.get("tags").get<picojson::object>();
        tags[DomainTag] = picojson::value{picojson::array{picojson::value{domain}}};
        def.get<picojson::object>()["tags"] = picojson::value{tags};
        return def.serialize();
    }

    void activations()
    {
        auto error = std::string{};
        check(connectedFlow(nullptr, Domain, error).empty() && error.empty(), "a deactivation reads nothing");
        check(connectedFlow(activation(Flow, Domain).c_str(), Domain, error) == Flow && error.empty(), "an activation reads the flow named");
        check(connectedFlow(activation(Flow, "").c_str(), Domain, error) == Flow && error.empty(),
            "a domain left to the node is the one it reads");
        check(connectedFlow(activation(Flow, Other).c_str(), Domain, error).empty() && !error.empty(), "another domain is not read");
        check(connectedFlow(activation(Flow, Domain).c_str(), "", error).empty() && !error.empty(),
            "a domain named to a node that does not know its own is not read");
        check(connectedFlow(activation(Flow, "").c_str(), "", error) == Flow && error.empty(),
            "a node that does not know its domain still reads a flow named alone");
        check(connectedFlow("{}", Domain, error).empty() && !error.empty(), "an activation without a flow id is refused");
        check(connectedFlow("nope", Domain, error).empty() && !error.empty(), "an activation that is no JSON is refused, not thrown on");
    }

    mxlMonitoringFlowSnapshot snapshot(char const* target)
    {
        auto s = mxlMonitoringFlowSnapshot{};
        s.version = 2;
        s.size = sizeof(s);
        std::strncpy(s.flowId, "cam1:video", sizeof(s.flowId) - 1);
        std::strncpy(s.target, target, sizeof(s.target) - 1);
        s.role = MXL_MONITORING_ROLE_READER;
        s.access = *target != '\0' ? MXL_MONITORING_ACCESS_OPEN : MXL_MONITORING_ACCESS_DISCONNECTED;
        s.opens = 1;
        s.delivering = *target != '\0';
        return s;
    }

    // What the poll hands a monitor: link, connection, synchronization and
    // stream, judged since the slot's previous poll.
    void statuses()
    {
        auto before = snapshot(Flow);
        auto now = before;
        auto s = judge(before, now);
        check(s[0].status == MXL_MONITORING_BCP008_HEALTHY && s[1].status == MXL_MONITORING_BCP008_HEALTHY &&
                  s[2].status == MXL_MONITORING_BCP008_INACTIVE,
            "a delivering slot has its link up, a healthy connection and no external synchronization");
        check(s[3].status == MXL_MONITORING_BCP008_UNCHANGED, "a slot that read nothing since leaves its stream status alone");

        now.skipped = 5;
        check(judge(before, now)[1].status == MXL_MONITORING_BCP008_UNHEALTHY, "grains lost since the poll before break the connection");
        check(judge(snapshot(Other), now)[1].status == MXL_MONITORING_BCP008_HEALTHY,
            "a slot moved to another flow is not judged on what it counted before");
        check(judge(std::nullopt, now)[1].status == MXL_MONITORING_BCP008_HEALTHY, "a first poll rests on access and delivery alone");

        auto const off = judge(before, snapshot(""));
        check(off[1].status == MXL_MONITORING_BCP008_INACTIVE && off[3].status == MXL_MONITORING_BCP008_INACTIVE,
            "a disconnected slot is inactive");
        now.access = MXL_MONITORING_ACCESS_DOMAIN_UNAVAILABLE;
        now.delivering = false;
        auto const down = judge(before, now);
        check(down[0].status == MXL_MONITORING_BCP008_UNHEALTHY && down[0].message[0] != '\0', "an unavailable domain is a link down, saying so");
    }

    void counting()
    {
        auto baseline = snapshot(Flow);
        baseline.skipped = 3;
        auto latest = baseline;
        latest.skipped = 10;
        latest.content[MXL_MONITORING_CONTENT_MISSING] = 2;
        latest.reads[MXL_MONITORING_READ_TOO_EARLY] = 4;
        auto const lost = counters(baseline, latest, MXL_MONITORING_BCP008_LOST);
        check(lost.size() == 2 && std::string{lost[0].name} == "skipped" && lost[0].value == 7 && lost[1].value == 2,
            "lost counts what was skipped and missing since the last reset");
        check(counters(std::nullopt, latest, MXL_MONITORING_BCP008_LOST)[0].value == 10, "without a reset lost counts from the start");
        auto const late = counters(baseline, latest, MXL_MONITORING_BCP008_LATE);
        check(late.size() == 1 && late[0].value == 4, "late counts reads too early past the grace");
        check(counters(baseline, latest, MXL_MONITORING_BCP008_TRANSMISSION_ERROR).empty(), "a receiver has no transmission errors");
    }
}

int main()
{
    rates();
    slots();
    definitions();
    activations();
    statuses();
    counting();
    if (failures == 0)
    {
        std::fprintf(stderr, "all NMOS checks pass\n");
    }
    return failures == 0 ? 0 : 1;
}
