FROM fedora@sha256:f4ca3e9814bef8468b59cc3dea90ab52bcb3747057731f7ab7645d923731fed6

ARG BUILD_TYPE=release
ARG COMPILER=gcc

#####
## go compiler
#####
ENV GOROOT=/usr/local/go \
    GOPATH=/usr/local/gopath \
    PATH=$PATH:/usr/local/go/bin \
    CGO_ENABLED=1
RUN curl -Ls https://dl.google.com/go/go1.12.3.linux-amd64.tar.gz | tar xzf - -C /usr/local

#####
## OS package dependencies
#####
COPY tools/install-deps.sh /

RUN /install-deps.sh && \
    rm /install-deps.sh && \
    dnf clean all

#####
## clang/3rdparty
#####
COPY 3rdparty.cmake.in CMakeLists.txt /rp/
COPY cmake/FindLibCxx.cmake /rp/cmake/
COPY cmake/caches/llvm.cmake /rp/cmake/caches/
COPY tools/build.py tools/build_helpers.py tools/clang.py tools/cli.py tools/constants.py tools/cpp.py tools/dependencies.py tools/fmt.py tools/fs.py tools/git.py tools/golang.py tools/llvm.py tools/log.py tools/packaging.py tools/pkg_config.py tools/shell.py tools/system.py tools/templates.py /rp/tools/
COPY tools/ci/scripts/install-external.sh /rp/tools/ci/scripts/

RUN cd /rp && \
    tools/ci/scripts/install-external.sh && \
    rm -r /rp

#####
## go deps
#####
COPY src/go/go.mod src/go/go.sum tools/ci/scripts/install-go-deps.sh /rpgo/

RUN cd /rpgo && \
    ./install-go-deps.sh && \
    rm -r /rpgo
