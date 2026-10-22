# dmf-mf-mxl-reader: consumer-side image for MXL flows.
#
# Builds mxl-reader inside a dmf-mxl/mxl tree, against the same
# go-mxl-builder/runtime pair as dmf-mf-mxl-writer and the mxl-k8s gateway,
# so reader, writer and gateway share one libmxl ABI and one flow layout.

# renovate: datasource=docker depName=ghcr.io/qvest-digital/go-mxl-builder
ARG GO_MXL_TAG=1.0.0-rc.12

# The stock dmf-mxl/mxl commit the reader builds against. Moves in lock-step
# with the libmxl go-mxl-runtime:${GO_MXL_TAG} ships.
ARG MXL_SRC=https://github.com/dmf-mxl/mxl.git
ARG MXL_REF=4a8a50e7

FROM ghcr.io/qvest-digital/go-mxl-builder:${GO_MXL_TAG} AS build
ARG MXL_REF
ARG MXL_SRC
# What the binary reports as its version: the release, or the commit.
ARG VERSION=dev

WORKDIR /src
# A full clone: GitHub refuses a shallow fetch of an arbitrary commit by SHA.
RUN git clone "${MXL_SRC}" . && git checkout "${MXL_REF}"

COPY src/ /src/tools/mxl-reader/
RUN echo 'add_subdirectory(mxl-reader)' >> tools/CMakeLists.txt

# go-mxl-builder ships vcpkg with CLI11, fmt and libfabric provisioned.
RUN cmake -S . -B build \
        -G Ninja \
        -DCMAKE_BUILD_TYPE=Release \
        -DMXL_ENABLE_FABRICS_OFI=ON \
        -DCMAKE_TOOLCHAIN_FILE=/root/vcpkg/scripts/buildsystems/vcpkg.cmake \
        -DMXL_READER_VERSION="${VERSION}" \
    && cmake --build build --target mxl-reader mxl-reader-tests mxl-reader-nmos-tests mxl-info -j \
    && build/tools/mxl-reader/mxl-reader-tests \
    && build/tools/mxl-reader/mxl-reader-nmos-tests

FROM ghcr.io/qvest-digital/go-mxl-runtime:${GO_MXL_TAG}

COPY --from=build /src/build/tools/mxl-reader/mxl-reader /usr/bin/mxl-reader
COPY --from=build /src/build/tools/mxl-info/mxl-info     /usr/bin/mxl-info

EXPOSE 9102
ENTRYPOINT ["/usr/bin/mxl-reader"]
