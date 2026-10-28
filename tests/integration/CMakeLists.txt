# Built inside the dmf-mxl/mxl tree as tools/mxl-reader-integration, beside
# tools/mxl-reader, by the image build alone; nothing here ships.

# Writes the faults integration-test.py checks mxl-reader against.
add_executable(mxl-pattern-writer)
target_compile_features(mxl-pattern-writer PRIVATE cxx_std_20)
target_sources(mxl-pattern-writer PRIVATE pattern-writer.cpp)

if (NOT TARGET CLI11::CLI11)
    find_package(CLI11 CONFIG REQUIRED)
endif ()

target_link_libraries(mxl-pattern-writer
        PRIVATE
            mxl
            mxl-monitoring
            mxl-monitoring-http
            CLI11::CLI11
    )
