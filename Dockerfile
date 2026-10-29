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
ARG MONITORING_TAG=v0.4.0
ARG MONITORING_REF=8a3ec560da3773319c788fe2ecd6d838784d1557

FROM ghcr.io/qvest-digital/go-mxl-builder:${GO_MXL_TAG} AS build
ARG MXL_REF
ARG MXL_SRC
ARG MONITORING_REF
# What the binary reports as its version: the release, or the commit.
ARG VERSION=dev

# opentelemetry-cpp with its OTLP/HTTP exporter, for libmxl-monitoring's OTLP
# exporter. go-mxl-builder does not carry it; installed from the image's vcpkg
# ports in classic mode, in a layer of its own that only a new GO_MXL_TAG
# rebuilds.
RUN /root/vcpkg/vcpkg install "opentelemetry-cpp[otlp-http]" --x-install-root=/opt/otel

WORKDIR /src
# A full clone: GitHub refuses a shallow fetch of an arbitrary commit by SHA.
RUN git clone "${MXL_SRC}" . && git checkout "${MXL_REF}"
RUN git clone https://github.com/marquies/libmxl-monitoring.git libmxl-monitoring \
 && git -C libmxl-monitoring checkout "${MONITORING_REF}" \
 && echo 'add_subdirectory(libmxl-monitoring)' >> CMakeLists.txt

COPY src/ /src/tools/mxl-reader/
COPY tests/unittests/ /src/tools/mxl-reader-unittests/
COPY tests/integration/ /src/tools/mxl-reader-integration/
RUN echo 'add_subdirectory(mxl-reader)' >> tools/CMakeLists.txt \
 && echo 'add_subdirectory(mxl-reader-unittests)' >> tools/CMakeLists.txt \
 && echo 'add_subdirectory(mxl-reader-integration)' >> tools/CMakeLists.txt

# go-mxl-builder ships vcpkg with CLI11, fmt and libfabric provisioned.
RUN cmake -S . -B build \
        -G Ninja \
        -DCMAKE_BUILD_TYPE=Release \
        -DMXL_ENABLE_FABRICS_OFI=ON \
        -DMXL_MONITORING_OTLP=ON \
        -DCMAKE_PREFIX_PATH=/opt/otel/x64-linux \
        -DCMAKE_TOOLCHAIN_FILE=/root/vcpkg/scripts/buildsystems/vcpkg.cmake \
        -DMXL_READER_VERSION="${VERSION}" \
    && cmake --build build --target mxl-reader mxl-reader-tests mxl-reader-nmos-tests mxl-pattern-writer mxl-info -j \
    && build/tools/mxl-reader-unittests/mxl-reader-tests \
    && build/tools/mxl-reader-unittests/mxl-reader-nmos-tests \
    && python3 tools/mxl-reader-integration/integration-test.py \
        build/tools/mxl-reader/mxl-reader build/tools/mxl-reader-integration/mxl-pattern-writer

FROM ghcr.io/qvest-digital/go-mxl-runtime:${GO_MXL_TAG}

COPY --from=build /src/build/tools/mxl-reader/mxl-reader /usr/bin/mxl-reader
COPY --from=build /src/build/tools/mxl-info/mxl-info     /usr/bin/mxl-info

EXPOSE 9102
ENTRYPOINT ["/usr/bin/mxl-reader"]
