# Built inside the dmf-mxl/mxl tree as tools/mxl-reader, so it links the
# same libmxl target and resolves its dependencies from the same toolchain as
# the upstream tools.

include(GNUInstallDirs)

add_executable(mxl-reader)
target_compile_features(mxl-reader PRIVATE cxx_std_20)
target_sources(mxl-reader PRIVATE mxl-reader.cpp)

# The release version, or the commit, the image is built from; the binary
# prints it at start and on --version.
set(MXL_READER_VERSION "dev" CACHE STRING "Version mxl-reader reports")
target_compile_definitions(mxl-reader PRIVATE MXL_READER_VERSION="${MXL_READER_VERSION}")

if (NOT TARGET CLI11::CLI11)
    find_package(CLI11 CONFIG REQUIRED)
endif ()

if (NOT TARGET fmt::fmt)
    find_package(fmt CONFIG REQUIRED)
endif ()

if (NOT TARGET picojson::picojson)
    find_package(picojson REQUIRED)
endif ()

# libmxl-monitoring, added to the same tree by the image build, counts and
# checks every read and renders /metrics.
target_link_libraries(mxl-reader
        PRIVATE
            mxl
            mxl-monitoring
            mxl-monitoring-prometheus
            CLI11::CLI11
            fmt::fmt
            picojson::picojson
    )

set_target_properties(mxl-reader
        PROPERTIES
            INSTALL_RPATH "${MXL_TOOLS_INSTALL_RPATH}"
    )
