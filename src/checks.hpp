// What mxl-reader decides about a flow and its grains without touching libmxl's
// state: parsing a flow definition and an --expect document, comparing the two,
// and classifying a grain or a batch of samples. Kept apart so it can be tested
// without a domain.

#pragma once

#include <cmath>
#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include <fmt/format.h>
#include <mxl/flow.h>
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

    // What a flow declares about itself, read from its flow definition.
    struct Declared
    {
        std::string mediaType;
        int width = 0;
        int height = 0;
        bool interlaced = false;
        std::string rate;
        int sampleRate = 0;
        int channels = 0;
    };

    inline int jsonInt(picojson::object const& object, char const* key)
    {
        auto const it = object.find(key);
        return (it != object.end() && it->second.is<double>()) ? static_cast<int>(it->second.get<double>()) : 0;
    }

    inline std::optional<Declared> parseDefinition(std::string const& text)
    {
        auto json = picojson::value{};
        if (!picojson::parse(json, text).empty() || !json.is<picojson::object>())
        {
            return std::nullopt;
        }
        auto const& object = json.get<picojson::object>();
        auto declared = Declared{};
        if (auto const it = object.find("media_type"); it != object.end() && it->second.is<std::string>())
        {
            declared.mediaType = it->second.get<std::string>();
        }
        if (auto const it = object.find("interlace_mode"); it != object.end() && it->second.is<std::string>())
        {
            declared.interlaced = it->second.get<std::string>().starts_with("interlaced");
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
    inline std::string mismatch(Declared const& declared, std::optional<Expectation> const& expected)
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

    // A v210 row packs six pixels into four 32-bit words of three 10-bit
    // samples, in 128-byte groups of 48 pixels.
    inline std::uint64_t v210RowBytes(int width)
    {
        return static_cast<std::uint64_t>((width + 47) / 48) * 128;
    }

    // An interlaced grain carries one field, half the frame's rows.
    inline int v210Rows(Declared const& declared)
    {
        return declared.interlaced ? declared.height / 2 : declared.height;
    }

    // v210 reserves the codes 0-3 and 1020-1023 for timing references, so a
    // picture sample there is not a legal value. Three rows are checked per
    // grain, the active pixels only: a row padded out to 48 pixels carries
    // zeros past the picture. Returns where the first illegal sample sits, or
    // "" when the rows are legal.
    inline std::string v210RowsIllegal(std::uint8_t const* payload, int width, int rows)
    {
        auto const rowBytes = v210RowBytes(width);
        auto const activeWords = static_cast<std::uint64_t>(width / 6) * 4;
        for (auto const row : {0, rows / 2, rows - 1})
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

    // Float audio outside -1..1, or not a number at all, across both wrapped
    // fragments of every channel.
    inline bool samplesLegal(mxlWrappedMultiBufferSlice const& slices)
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

    // The one outcome a grain lands in, worst first, so a grain is counted
    // once whatever else is wrong with it.
    enum class GrainOutcome
    {
        Ok,
        WrongFormat,
        Missing,
        Invalid,
        Incomplete,
        WrongSize,
        Illegal,
    };

    // libmxl hands back whatever the ring slot holds once the index is within
    // the ring, so a grain whose own index differs is one the writer or the
    // mirror never wrote: the slot still holds an older cycle's grain.
    inline GrainOutcome classifyGrain(mxlGrainInfo const& grain, std::uint64_t index, std::uint8_t const* payload,
        Declared const& declared, bool wrongFormat, bool checkLegal, std::string* where)
    {
        if (wrongFormat)
        {
            return GrainOutcome::WrongFormat;
        }
        if (grain.index != index)
        {
            return GrainOutcome::Missing;
        }
        if ((grain.flags & MXL_GRAIN_FLAG_INVALID) != 0)
        {
            return GrainOutcome::Invalid;
        }
        if (grain.validSlices < grain.totalSlices)
        {
            return GrainOutcome::Incomplete;
        }
        auto const v210 = declared.mediaType == "video/v210" && declared.width > 0 && declared.height > 0;
        if (v210 && grain.grainSize != v210RowBytes(declared.width) * static_cast<std::uint64_t>(v210Rows(declared)))
        {
            return GrainOutcome::WrongSize;
        }
        if (v210 && checkLegal && payload != nullptr)
        {
            if (auto found = v210RowsIllegal(payload, declared.width, v210Rows(declared)); !found.empty())
            {
                if (where != nullptr)
                {
                    *where = std::move(found);
                }
                return GrainOutcome::Illegal;
            }
        }
        return GrainOutcome::Ok;
    }

    // A Prometheus label value from a writer-controlled string: anything
    // outside a media type's own characters would break the exposition.
    inline std::string labelValue(std::string const& value)
    {
        auto out = std::string{};
        for (auto const c : value)
        {
            auto const ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '/' || c == '.' ||
                            c == '_' || c == '+' || c == '-';
            out += ok ? c : '_';
        }
        return out;
    }
}
