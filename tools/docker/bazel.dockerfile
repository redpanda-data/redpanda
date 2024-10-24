ARG BASE_IMAGE_OS_NAME=fedora
ARG BASE_IMAGE_OS_VERSION=38
FROM ${BASE_IMAGE_OS_NAME}:${BASE_IMAGE_OS_VERSION}

COPY --chown=0:0 bazel/install-deps.sh /

RUN dnf install -y wget || { apt-get update && apt install -y wget; }
# convenience for CI that runs sysctl to bump max-aio-nr for testing
# ubuntu already has this installed by default
RUN dnf install -y procps-ng || true
RUN wget -O /usr/local/bin/bazel \
        https://github.com/bazelbuild/bazelisk/releases/latest/download/bazelisk-linux-amd64 && \
        chmod +x /usr/local/bin/bazel

# run after wget installation to take advantage of pkg cache cleaning
RUN CLEAN_PKG_CACHE=true /install-deps.sh && rm /install-deps.sh
