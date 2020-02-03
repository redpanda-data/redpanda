FROM fedora@sha256:8fa60b88e2a7eac8460b9c0104b877f1aa0cea7fbc03c701b7e545dacccfb433

ARG BUILD_TYPE=release
ARG COMPILER=gcc
ENV CI=1
WORKDIR /v

# OS package dependencies
COPY tools/install-deps.sh /
RUN rm -f /etc/yum.repos.d/fedora-* && \
    /install-deps.sh && \
    rm /install-deps.sh && \
    dnf clean all

# vtools
COPY tools/setup.py tools/README.md /v/tools/
COPY tools/vtools/ /v/tools/vtools
RUN pip install /v/tools && \
    rm -fr /v/build/

# vtools config for CI
COPY tools/ci/vtools-${COMPILER}-${BUILD_TYPE}.yml /v/.vtools.yml

# go compiler
RUN vtools install go-compiler

# go modules' dependencies
COPY src/go/rpk/go.mod src/go/rpk/go.sum /v/src/go/rpk/
COPY src/go/metrics/go.mod src/go/metrics/go.sum /v/src/go/metrics/
RUN vtools install go-deps

# clang/3rdparty
COPY 3rdparty.cmake.in CMakeLists.txt /v/
COPY cmake/caches/llvm.cmake /v/cmake/caches/
RUN bash -c 'if [[ "$COMPILER" == "clang" ]]; then vtools install clang; fi' && \
    vtools install cpp-deps && \
    rm -r /v/
