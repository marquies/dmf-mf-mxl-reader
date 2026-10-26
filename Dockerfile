# dmf-mf-mxl-reader: consumer-side image for MXL flows.
#
# Builds mxl-reader inside a dmf-mxl/mxl tree, against the same
# go-mxl-builder/runtime pair as dmf-mf-mxl-writer and the mxl-k8s gateway,
# so reader, writer and gateway share one libmxl ABI and one flow layout.

# renovate: datasource=docker depName=ghcr.io/qvest-digital/go-mxl-builder
ARG GO_MXL_TAG=1.1.0-rc.7

# MXL_SRC and MXL_REF are the repository and commit named in go-mxl's
# .github/libmxl.version at GO_MXL_TAG: the libmxl go-mxl-runtime ships.
ARG MXL_SRC=https://github.com/qvest-digital/mxl.git
ARG MXL_REF=c259243931333497e61faabeedac8048915dcb2b

# libmxl-monitoring counts, checks and renders what the reader reads; built
# into the same tree and linked statically. Checked out by commit, which a
# moved tag cannot change; the tag names the release it is.
# renovate: datasource=github-tags depName=marquies/libmxl-monitoring
ARG MONITORING_TAG=v0.2.0
ARG MONITORING_REF=c034ebf78a97a0d65e217021bff1ad693c9639c3

FROM ghcr.io/qvest-digital/go-mxl-builder:${GO_MXL_TAG} AS build
ARG MXL_REF
ARG MXL_SRC
ARG MONITORING_REF
# What the binary reports as its version: the release, or the commit.
ARG VERSION=dev

WORKDIR /src
# A full clone: GitHub refuses a shallow fetch of an arbitrary commit by SHA.
RUN git clone "${MXL_SRC}" . && git checkout "${MXL_REF}"
RUN git clone https://github.com/marquies/libmxl-monitoring.git libmxl-monitoring \
 && git -C libmxl-monitoring checkout "${MONITORING_REF}" \
 && echo 'add_subdirectory(libmxl-monitoring)' >> CMakeLists.txt

COPY src/ /src/tools/mxl-reader/
COPY tests/unittests/ /src/tools/mxl-reader-unittests/
RUN echo 'add_subdirectory(mxl-reader)' >> tools/CMakeLists.txt \
 && echo 'add_subdirectory(mxl-reader-unittests)' >> tools/CMakeLists.txt

# go-mxl-builder ships vcpkg with CLI11, fmt and libfabric provisioned.
RUN cmake -S . -B build \
        -G Ninja \
        -DCMAKE_BUILD_TYPE=Release \
        -DMXL_ENABLE_FABRICS_OFI=ON \
        -DCMAKE_TOOLCHAIN_FILE=/root/vcpkg/scripts/buildsystems/vcpkg.cmake \
        -DMXL_READER_VERSION="${VERSION}" \
    && cmake --build build --target mxl-reader mxl-reader-tests mxl-reader-nmos-tests mxl-info -j \
    && build/tools/mxl-reader-unittests/mxl-reader-tests \
    && build/tools/mxl-reader-unittests/mxl-reader-nmos-tests

FROM ghcr.io/qvest-digital/go-mxl-runtime:${GO_MXL_TAG}

COPY --from=build /src/build/tools/mxl-reader/mxl-reader /usr/bin/mxl-reader
COPY --from=build /src/build/tools/mxl-info/mxl-info     /usr/bin/mxl-info

EXPOSE 9102
ENTRYPOINT ["/usr/bin/mxl-reader"]
