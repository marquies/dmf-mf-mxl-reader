// A writer for integration-test.py: keeps a video and an audio flow written at
// the media clock and, on command, writes the faults a reader has to tell
// apart. One command per line on stdin; each answers "done" on stdout once
// what it describes has been written, "skipped" for a fault while the flows
// are removed, or "unknown" for a command it does not know. Writing goes on
// between commands.
//
//   pause <seconds>     write nothing, the flows stay
//   invalid <n>         the next n grains flagged MXL_GRAIN_FLAG_INVALID
//   partial <n>         the next n grains with half their slices
//   illegal <n>         the next n grains with v210 code 0, which is reserved
//   size <n>            the next n grains claiming one line less than their
//                       format needs
//   remove              release the writers; libmxl deletes a flow when its
//                       last writer is released
//   create              create the writers again, unless they are there
//   quit
//
// A grain is written as mxl-gst-testsrc writes one: the header libmxl hands
// out is kept and only validSlices set, so whatever libmxl leaves in it, such
// as an earlier grain's invalid flag, reaches the reader. The size is the
// exception: libmxl keeps a slot's size as the last commit stored it, and a
// writer always writes its flow's size, so after a size fault the slot is
// right again.

#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <poll.h>
#include <unistd.h>
#include <CLI/CLI.hpp>
#include <mxl/flow.h>
#include <mxl/mxl.h>
#include <mxl/time.h>

namespace
{
    enum class Fault
    {
        None,
        Invalid,
        Partial,
        Illegal,
        Size,
    };

    std::string readFile(std::string const& path)
    {
        auto in = std::ifstream{path};
        auto text = std::stringstream{};
        text << in.rdbuf();
        return text.str();
    }

    struct Writer
    {
        std::string domain;
        std::string videoDef;
        std::string audioDef;
        mxlInstance instance = nullptr;
        mxlFlowWriter video = nullptr;
        mxlFlowWriter audio = nullptr;
        mxlRational videoRate{};
        mxlRational audioRate{};
        std::uint64_t nextGrain = 0;
        // Taken from the first grain opened after the flow is created.
        std::uint32_t grainSize = 0;
        std::uint64_t samplesTo = 0;

        bool create()
        {
            auto config = mxlFlowConfigInfo{};
            if (mxlCreateFlowWriter(instance, videoDef.c_str(), "", &video, &config, nullptr) != MXL_STATUS_OK)
            {
                return false;
            }
            videoRate = config.common.grainRate;
            nextGrain = mxlGetCurrentIndex(&videoRate);
            grainSize = 0;
            if (mxlCreateFlowWriter(instance, audioDef.c_str(), "", &audio, &config, nullptr) != MXL_STATUS_OK)
            {
                // Both flows or neither.
                mxlReleaseFlowWriter(instance, video);
                video = nullptr;
                return false;
            }
            audioRate = config.common.grainRate;
            samplesTo = mxlGetCurrentIndex(&audioRate);
            return true;
        }

        void release()
        {
            mxlReleaseFlowWriter(instance, video);
            mxlReleaseFlowWriter(instance, audio);
            video = audio = nullptr;
        }

        bool writeGrain(std::uint64_t index, Fault fault)
        {
            auto info = mxlGrainInfo{};
            std::uint8_t* payload = nullptr;
            if (mxlFlowWriterOpenGrain(video, index, &info, &payload) != MXL_STATUS_OK)
            {
                return false;
            }
            if (grainSize == 0)
            {
                grainSize = info.grainSize;
            }
            info.grainSize = grainSize;
            // Mid grey, Y and both chroma at code 512, unless the fault is a
            // reserved code.
            auto const word = fault == Fault::Illegal ? std::uint32_t{0} : std::uint32_t{512U | (512U << 10) | (512U << 20)};
            for (std::size_t i = 0; i + 4 <= info.grainSize; i += 4)
            {
                std::memcpy(payload + i, &word, 4);
            }
            switch (fault)
            {
                case Fault::Invalid:
                    // As mxl-gst-testsrc fills a gap: the flag set, nothing else.
                    info.flags = MXL_GRAIN_FLAG_INVALID;
                    break;
                case Fault::Partial: info.validSlices = info.totalSlices / 2; break;
                case Fault::Size:
                    // commit stores the header as given, size included.
                    info.validSlices = info.totalSlices;
                    info.grainSize -= info.grainSize / info.totalSlices;
                    break;
                default:             info.validSlices = info.totalSlices; break;
            }
            return mxlFlowWriterCommitGrain(video, &info) == MXL_STATUS_OK;
        }

        bool writeSamples(std::uint64_t to)
        {
            auto maxLength = std::size_t{0};
            mxlFlowWriterGetMaxWriteLengthSamples(audio, &maxLength);
            while (samplesTo < to)
            {
                auto const count = std::min<std::uint64_t>(to - samplesTo, maxLength);
                auto slices = mxlMutableWrappedMultiBufferSlice{};
                if (mxlFlowWriterOpenSamples(audio, samplesTo + count, count, &slices) != MXL_STATUS_OK)
                {
                    return false;
                }
                for (std::size_t channel = 0; channel < slices.count; ++channel)
                {
                    for (auto const& fragment : slices.base.fragments)
                    {
                        if (fragment.pointer != nullptr)
                        {
                            std::memset(static_cast<std::uint8_t*>(fragment.pointer) + channel * slices.stride, 0, fragment.size);
                        }
                    }
                }
                if (mxlFlowWriterCommitSamples(audio) != MXL_STATUS_OK)
                {
                    return false;
                }
                samplesTo += count;
            }
            return true;
        }

        // Writes everything due up to now, the first `faulty` grains of it
        // with the fault.
        bool catchUp(Fault fault, std::uint64_t& faulty)
        {
            for (auto const due = mxlGetCurrentIndex(&videoRate); nextGrain <= due; ++nextGrain)
            {
                if (!writeGrain(nextGrain, faulty > 0 ? fault : Fault::None))
                {
                    return false;
                }
                faulty -= faulty > 0 ? 1 : 0;
            }
            return writeSamples(mxlGetCurrentIndex(&audioRate));
        }

        // After a pause: the indexes not written are left behind.
        void skip()
        {
            nextGrain = mxlGetCurrentIndex(&videoRate);
            samplesTo = mxlGetCurrentIndex(&audioRate);
        }
    };
}

int main(int argc, char** argv)
{
    auto app = CLI::App{"Writes the faults integration-test.py checks the reader against."};
    auto writer = Writer{};
    auto videoPath = std::string{};
    auto audioPath = std::string{};
    app.add_option("-d,--domain", writer.domain, "The MXL domain directory.")->required();
    app.add_option("-v,--video", videoPath, "Video flow definition.")->required();
    app.add_option("-a,--audio", audioPath, "Audio flow definition.")->required();
    CLI11_PARSE(app, argc, argv);

    writer.videoDef = readFile(videoPath);
    writer.audioDef = readFile(audioPath);
    writer.instance = mxlCreateInstance(writer.domain.c_str(), "");
    if (writer.instance == nullptr || !writer.create())
    {
        std::cerr << "pattern-writer: cannot create the flows in " << writer.domain << "\n";
        return 1;
    }
    std::cout << "ready" << std::endl;

    auto const seconds = [](double s) { return static_cast<std::uint64_t>(s * 1e9); };
    auto const reply = [](bool done) { std::cout << (done ? "done" : "failed") << std::endl; };
    auto const created = [&writer] { return writer.video != nullptr; };

    // What the current command still waits for: the end of a pause or the
    // faulty grains still to write.
    auto pausedUntil = std::uint64_t{0};
    auto fault = Fault::None;
    auto faulty = std::uint64_t{0};
    auto busy = false;
    auto input = std::string{};
    auto ok = true;
    auto quit = false;
    while (ok && !quit)
    {
        auto const now = mxlGetTime();
        if (busy && pausedUntil != 0 && now >= pausedUntil)
        {
            writer.skip();
            pausedUntil = 0;
            busy = false;
            reply(true);
        }
        if (writer.video != nullptr && pausedUntil == 0)
        {
            auto const wasFaulty = faulty;
            ok = ok && writer.catchUp(fault, faulty);
            if (busy && wasFaulty > 0 && faulty == 0)
            {
                busy = false;
                reply(ok);
            }
        }

        // A command is taken only once the last one is done, and more input
        // read only once every command read is taken, so the end of input
        // never cuts off commands before it.
        auto fd = pollfd{.fd = STDIN_FILENO, .events = POLLIN, .revents = 0};
        if (!busy && input.find('\n') == std::string::npos && ::poll(&fd, 1, 0) > 0)
        {
            char buffer[256];
            auto const n = ::read(STDIN_FILENO, buffer, sizeof buffer);
            if (n <= 0)
            {
                break;
            }
            input.append(buffer, static_cast<std::size_t>(n));
        }
        auto const eol = input.find('\n');
        if (!busy && eol != std::string::npos)
        {
            auto in = std::istringstream{input.substr(0, eol)};
            input.erase(0, eol + 1);
            auto command = std::string{};
            auto value = 0.0;
            in >> command >> value;
            busy = true;
            if (command == "pause")
            {
                pausedUntil = now + seconds(value);
            }
            else if (command == "invalid" || command == "partial" || command == "illegal" || command == "size")
            {
                fault = command == "invalid"   ? Fault::Invalid
                        : command == "partial" ? Fault::Partial
                        : command == "illegal" ? Fault::Illegal
                                               : Fault::Size;
                faulty = static_cast<std::uint64_t>(value);
                // No grain to write it into: waiting for one would hold every
                // later command, the create that brings the flows back too.
                if (!created() || faulty == 0)
                {
                    faulty = 0;
                    busy = false;
                    std::cout << (created() ? "done" : "skipped") << std::endl;
                }
            }
            else if (command == "remove")
            {
                if (created())
                {
                    writer.release();
                }
                busy = false;
                reply(true);
            }
            else if (command == "create")
            {
                // A domain that is not there fails this create; the next one
                // may find it back.
                busy = false;
                reply(created() || writer.create());
            }
            else if (command == "quit")
            {
                quit = true;
            }
            else
            {
                std::cerr << "pattern-writer: unknown command " << command << "\n";
                busy = false;
                std::cout << "unknown" << std::endl;
            }
        }
        mxlSleepForNs(2'000'000);
    }
    if (writer.video != nullptr)
    {
        writer.release();
    }
    mxlDestroyInstance(writer.instance);
    return ok ? 0 : 1;
}
