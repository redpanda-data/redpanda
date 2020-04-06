# fedora:31
FROM fedora@sha256:8fa60b88e2a7eac8460b9c0104b877f1aa0cea7fbc03c701b7e545dacccfb433 AS base

ARG BUILD_TYPE=release
ARG COMPILER=gcc
ENV CI=1
WORKDIR /v

COPY tools/install-deps.sh /
COPY tools/ci/vtools-${COMPILER}-${BUILD_TYPE}.yml /v/.vtools.yml
RUN rm -f /etc/yum.repos.d/fedora-* && \
    /install-deps.sh && \
    rm /install-deps.sh && \
    dnf clean all && \
    curl -fsSLO https://download.docker.com/linux/static/stable/x86_64/docker-19.03.8.tgz && \
    tar xzvf docker-19.03.8.tgz --strip 1 -C /usr/local/bin docker/docker && \
    rm docker-19.03.8.tgz

# python:3.7.7-slim-buster
FROM python@sha256:994806eb256bc1554ca381cf52c0ba5612192f42b7f93a93e91f9fdb6078ec52 AS vtools

COPY tools/setup.py tools/README.md /v/tools/
COPY tools/vtools/ /v/tools/vtools
RUN pip install pex && \
    pex /v/tools -c vtools -o /vectorized/bin/vtools -v && \
    pex yapf -c yapf -o /vectorized/bin/yapf -v && \
    rm -r /v

FROM base as godeps

COPY --from=vtools /vectorized/bin/* /usr/bin/
COPY src/go/rpk/go.mod src/go/rpk/go.sum /v/src/go/rpk/
COPY src/go/metrics/go.mod src/go/metrics/go.sum /v/src/go/metrics/
RUN vtools install go-compiler && \
    vtools install go-deps && \
    rm -r /v

FROM base as cppdeps

COPY --from=vtools /vectorized/bin/* /usr/bin/
COPY 3rdparty.cmake.in CMakeLists.txt /v/
COPY cmake/caches/llvm.cmake /v/cmake/caches/
RUN bash -c 'if [[ "$COMPILER" == "clang" ]]; then vtools install clang; fi' && \
    vtools install cpp-deps && \
    rm -r /v

COPY --from=godeps /vectorized/go /vectorized/go
