# Built inside the dmf-mxl/mxl tree as tools/mxl-reader-unittests, beside
# tools/mxl-reader, whose headers the tests include.

set(MXL_READER_SOURCE_DIR "${CMAKE_SOURCE_DIR}/tools/mxl-reader")

if (NOT TARGET fmt::fmt)
    find_package(fmt CONFIG REQUIRED)
endif ()

if (NOT TARGET picojson::picojson)
    find_package(picojson REQUIRED)
endif ()

add_executable(mxl-reader-tests)
target_compile_features(mxl-reader-tests PRIVATE cxx_std_20)
target_sources(mxl-reader-tests PRIVATE checks-test.cpp)
target_include_directories(mxl-reader-tests PRIVATE "${MXL_READER_SOURCE_DIR}")
target_link_libraries(mxl-reader-tests
        PRIVATE
            mxl
            mxl-monitoring
            fmt::fmt
            picojson::picojson
    )

add_executable(mxl-reader-nmos-tests)
target_compile_features(mxl-reader-nmos-tests PRIVATE cxx_std_20)
target_sources(mxl-reader-nmos-tests PRIVATE nmos-test.cpp)
target_include_directories(mxl-reader-nmos-tests PRIVATE "${MXL_READER_SOURCE_DIR}")
target_link_libraries(mxl-reader-nmos-tests
        PRIVATE
            fmt::fmt
            picojson::picojson
    )
