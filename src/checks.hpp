// What mxl-reader is told to expect of each flow: the --expect document, and
// the expectation the monitoring library checks it against. Kept apart so it
// can be tested without a domain.

#pragma once

#include <charconv>
#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <vector>

#include <mxl-monitoring/content.h>
#include <picojson/wrapper.h>

namespace reader
{
    // What a reader was told to expect of a flow. Unset fields are not checked.
    struct Expectation
    {
        std::string mediaType;
        int width = 0;
        int height = 0;
        std::string rate;
        int sampleRate = 0;
        int channels = 0;
        // Check every sample against the legal range. Off unless asked: a test
        // pattern may carry reserved codes on purpose, SMPTE bars write
        // super-black as luma code 0, and a check that always fires hides the
        // faults it is there to find.
        bool checkLegal = false;
    };

    inline int jsonInt(picojson::object const& object, char const* key)
    {
        auto const it = object.find(key);
        return (it != object.end() && it->second.is<double>()) ? static_cast<int>(it->second.get<double>()) : 0;
    }

    // --expect '{"<flow id>": {"mediaType": "video/v210", "width": 1920,
    // "height": 1080, "rate": "50/1", "checkLegal": true}, ...}'; audio takes
    // sampleRate and channels. A key the reader does not know, or a flow it is
    // not reading, is refused: a misspelt expectation would otherwise check
    // nothing and say nothing. Returns an error, or "" when the document is
    // usable.
    inline std::string parseExpectations(std::string const& text, std::vector<std::string> const& flowIds,
        std::map<std::string, Expectation>& out)
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
        auto const reading = std::set<std::string>(flowIds.begin(), flowIds.end());
        static std::set<std::string> const known = {"mediaType", "width", "height", "rate", "sampleRate", "channels", "checkLegal"};
        for (auto const& [id, value] : json.get<picojson::object>())
        {
            if (!reading.contains(id))
            {
                return "--expect names " + id + ", which is not a flow this reader reads";
            }
            if (!value.is<picojson::object>())
            {
                return "--expect: the expectation for " + id + " must be an object";
            }
            auto const& o = value.get<picojson::object>();
            for (auto const& [key, _] : o)
            {
                if (!known.contains(key))
                {
                    return "--expect: " + key + " is not something the reader checks";
                }
            }
            auto e = Expectation{};
            if (auto const it = o.find("mediaType"); it != o.end() && it->second.is<std::string>())
            {
                e.mediaType = it->second.get<std::string>();
            }
            if (auto const it = o.find("rate"); it != o.end() && it->second.is<std::string>())
            {
                e.rate = it->second.get<std::string>();
            }
            if (auto const it = o.find("checkLegal"); it != o.end() && it->second.is<bool>())
            {
                e.checkLegal = it->second.get<bool>();
            }
            e.width = jsonInt(o, "width");
            e.height = jsonInt(o, "height");
            e.sampleRate = jsonInt(o, "sampleRate");
            e.channels = jsonInt(o, "channels");
            out[id] = e;
        }
        return {};
    }

    // The library's form of an expectation. mediaType points into e, which
    // has to outlive the result. A rate is "<numerator>/<denominator>",
    // validated before; anything else is not checked.
    inline mxlMonitoringExpectation toMonitoring(Expectation const& e)
    {
        auto out = mxlMonitoringExpectation{};
        out.version = 1;
        out.size = sizeof(out);
        out.mediaType = e.mediaType.empty() ? nullptr : e.mediaType.c_str();
        out.width = e.width;
        out.height = e.height;
        if (auto const slash = e.rate.find('/'); slash != std::string::npos)
        {
            auto n = std::int64_t{0}, d = std::int64_t{0};
            std::from_chars(e.rate.data(), e.rate.data() + slash, n);
            std::from_chars(e.rate.data() + slash + 1, e.rate.data() + e.rate.size(), d);
            out.rate = mxlRational{n, d};
        }
        out.sampleRate = e.sampleRate;
        out.channels = e.channels;
        out.checkLegal = e.checkLegal;
        return out;
    }
}
