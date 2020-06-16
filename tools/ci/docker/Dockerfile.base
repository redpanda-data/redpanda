# fedora:31
FROM fedora@sha256:8fa60b88e2a7eac8460b9c0104b877f1aa0cea7fbc03c701b7e545dacccfb433 AS base

ENV CI=1

WORKDIR /v

COPY tools/install-deps.sh /

RUN rm -f /etc/yum.repos.d/fedora-* && \
    /install-deps.sh && \
    rm /install-deps.sh && \
    dnf clean all && \
    curl -fsSLO https://download.docker.com/linux/static/stable/x86_64/docker-19.03.8.tgz && \
    tar xzvf docker-19.03.8.tgz --strip 1 -C /usr/local/bin docker/docker && \
    rm docker-19.03.8.tgz

############

FROM base as godeps

ARG GITHUB_API_TOKEN

# go dependencies are built with gcc, and are not affected by build-type
COPY tools/ci/vtools-gcc-release.yml /v/.vtools.yml

# vtools has lots of functionality that is irrelevant to the code path that installs 
# go dependencies. In order to avoid having the cache being invalidated if we do:
#
#   COPY tools/vtools /v/tools/
#
# we instead install from github (pip install git+https://...) so that the build 
# always hits the cache, as the URL is always the same (upstream repo), which 
# docker/kaniko interpret as not changing. However, we do explicitly add the 
# modules that are relevant for the 'vtools install go-deps' command so that if 
# this logic changes, the cache is properly invalidated.
COPY tools/vtools/install/commands.py tools/vtools/vlib/config.py /v/

COPY src/go/rpk/go.mod src/go/rpk/go.sum /v/src/go/rpk/
COPY src/go/metrics/go.mod src/go/metrics/go.sum /v/src/go/metrics/

RUN pip install --no-cache-dir "git+https://${GITHUB_API_TOKEN}@github.com/vectorizedio/v@dev#egg=vtools&subdirectory=tools/" && \
    cp commands.py /usr/local/lib/python3.7/site-packages/vtools/install/ && \
    cp config.py /usr/local/lib/python3.7/site-packages/vtools/vlib/ && \
    vtools install go-compiler && \
    vtools install go-deps && \
    rm -r /v

############

FROM base as clang

ARG GITHUB_API_TOKEN

# clang is always built with gcc and is not affected by build-type
COPY tools/ci/vtools-clang-release.yml /v/.vtools.yml

# same as above (godeps), we COPY only the module relevant for installing clang
COPY tools/vtools/vlib/clang.py tools/vtools/vlib/config.py tools/vtools/install/commands.py /v/

COPY cmake/caches/llvm.cmake /v/cmake/caches/

# re-install vtools in case the base image wasn't rebuilt
RUN pip install --no-cache-dir "git+https://${GITHUB_API_TOKEN}@github.com/vectorizedio/v@dev#egg=vtools&subdirectory=tools/" && \
    cp commands.py /usr/local/lib/python3.7/site-packages/vtools/install/ && \
    cp config.py clang.py /usr/local/lib/python3.7/site-packages/vtools/vlib/ && \
    vtools install clang && \
    rm -r /v

############

FROM clang as cppdeps

ARG COMPILER
ARG BUILD_TYPE
ARG GITHUB_API_TOKEN

# same as above (godeps), we COPY only modules relevant for installing cpp-deps
COPY tools/vtools/vlib/cmake.py tools/vtools/vlib/config.py tools/vtools/install/commands.py /v/

COPY 3rdparty.cmake.in CMakeLists.txt /v/
COPY tools/ci/vtools-${COMPILER}-${BUILD_TYPE}.yml /v/.vtools.yml

# note we properly cleanup by removing vtools, since it is only used to build 
# dependencies and not needed outside the image.
RUN pip install --no-cache-dir "git+https://${GITHUB_API_TOKEN}@github.com/vectorizedio/v@dev#egg=vtools&subdirectory=tools/" && \
    cp commands.py /usr/local/lib/python3.7/site-packages/vtools/install/ && \
    cp config.py cmake.py /usr/local/lib/python3.7/site-packages/vtools/vlib/ && \
    vtools install clang --fetch && \
    vtools install cpp-deps && \
    pip install pip-autoremove && \
    pip-autoremove -y vtools && \
    rm -r /v

COPY --from=godeps /vectorized/go /vectorized/go
