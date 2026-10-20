// Tests of what mxl-reader decides about a flow and its grains. Built and run
// by the image build, so an image carrying a wrong classification is never
// published.

#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "checks.hpp"

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
}

int main()
{
    definitions();
    expectations();
    grains();
    samples();
    labels();
    if (failures == 0)
    {
        std::fprintf(stderr, "all checks pass\n");
    }
    return failures == 0 ? 0 : 1;
}
