ARG GOLANG_SHA=latest
ARG CLANG_SHA=latest
FROM gcr.io/redpandaci/golang:${GOLANG_SHA} as godeps
FROM gcr.io/redpandaci/clang:${CLANG_SHA} as clang

ARG COMPILER=gcc
ARG BUILD_TYPE=release

COPY 3rdparty.cmake.in CMakeLists.txt /v/
COPY tools /v/tools/

RUN pip install /v/tools && \
    cp tools/ci/vtools-${COMPILER}-${BUILD_TYPE}.yml /v/.vtools.yml && \
    vtools install clang --fetch && \
    vtools install cpp-deps && \
    pip uninstall -y vtools && \
    rm -r /v

COPY --from=godeps /vectorized/go /vectorized/go
